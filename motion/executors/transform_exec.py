import asyncio
import concurrent.futures
import datetime
import dataclasses
import inspect
import multiprocessing
# import ray

from motion.transform import TransformV2 as Transform
# from ray.util.actor_pool import ActorPool
# from ray.util.queue import Queue


def _get_fields_from_type(t):
    if not t:
        raise ValueError("Cannot get fields from None type.")
    return dataclasses.fields(t)

# @ray.remote
# class TransformWrapper:
#     def __init__(self, transform):
#         self.transform = transform
    
#     def infer(self, feature):
#         return self.transform.infer(feature)

class TransformVersion(object):
    def __init__(self, te, version):
        self.te = te
        self.timestamp = datetime.datetime.now()
        
        # Set up transform object
        self.transform = self.te.transform()
        # Set user-defined parameters
        self.min_train_size = (
            self.transform.min_train_size
            if hasattr(self.transform, "min_train_size")
            else 0
        )
        self.ignore_fit = (
            self.transform.ignore_fit
            if hasattr(self.transform, "ignore_fit")
            else False
        )
        self.max_staleness = (
            self.transform.max_staleness
            if hasattr(self.transform, "max_staleness")
            else 0
        )
        if self.ignore_fit:
            self.max_staleness = 1e10
        
        self.inferBatchExists = False
        if type(self.transform).inferBatch is not Transform.inferBatch:
            self.inferBatchExists = True
        
        self.cache = {}
        self.should_process = []
        self.processed = asyncio.Queue()
        
        self.fit_task = asyncio.create_task(self.fit(version, self.ignore_fit))
    
    async def fit(self, version, ignore_fit):
        if not ignore_fit:
            train_ids = self.te.store.idsBefore(version)
            if len(train_ids) < self.transform.min_train_size:
                raise ValueError(
                    f"Insufficient data to train {version}. Need at least {self.min_train_size} datapoints."
                )
            labels = None

            assert version not in train_ids
            
            features = await self.te.fetchFeatures(train_ids, version=version)

            if self.transform.labelType is not None:
                labels = []
                for train_id in train_ids:
                    label_values = self.te.store.mget(
                        train_id,
                        self.te.label_fields,
                    )
                    labels.append(
                        self.transform.labelType(
                            **{
                                k: v
                                for k, v in label_values.items()
                                if v is not None
                            }
                        )
                    )

            # Fit transform to training set
            self.transform._check_type(features=features, labels=labels)
            self.transform.fit(features=features, labels=labels)
        
        # self.transform_ref = ray.put(self.transform)
        # self.actors = [TransformWrapper.remote(self.transform_ref) for _ in range(4)]
        # self.pool = ActorPool(self.actors)
    
    async def infer(self, id):
        await self.fit_task
        
        if id in self.cache:
            return self.cache[id]
        
         # Get features for this id
        features = await self.te.fetchFeatures([id], version=id)
        features = features[0]
        
        # Run inference
        # result_ref = fn.remote(self.transform_ref, self.state_ref, features)
        # res = ray.get([result_ref])[0]
        res = self.transform.infer(features)
        self.cache[id] = res   
        
        # Log results to processed queue
        self.processed.put_nowait((id, features, res))
        
        return res
    
    async def inferMany(self, ids):
        await self.fit_task
        
        # Get features for these ids
        fetch_tasks = [self.te.fetchFeatures([id], version=id) for id in ids]
        all_features = await asyncio.gather(*fetch_tasks)
        
    
        
        if self.inferBatchExists:
            results = self.transform.inferBatch([f[0] for f in all_features])
            all_results = {id: res for id, res in zip(ids, results)}
        
        else:
            # RAY
            # gen = self.pool.map(lambda a, f: a.infer.remote(f[0]), all_features)
            # results = list(gen)
            # all_results = {id: res for id, res in zip(ids, results)}
            all_results = {id: self.transform.infer(self.state, features[0]) for id, features in zip(ids, all_features)}
        
        # TODO: put in queue
        
        return all_results
        
    async def getProcessed(self):
        while True:
            item = await self.processed.get()
            if item[0] == id:
                return item
    
    async def processOutstanding(self):
        results = await self.inferMany(self.should_process)
        self.should_process = []
        return results
    
    def __repr__(self):
        return f"TransformVersion({self.timestamp}, {self.transform.state})"

class TransformExecutorV2(object):
    def __init__(self, transform, store, upstream_executors=[]):
        self.transform = transform
        self.store = store
        self.upstream_executors = upstream_executors
        
        # Set state history and types
        # self.state_history = multiprocessing.Manager().dict()
        self.state_history = {}
        self.feature_fields = [
            f.name for f in _get_fields_from_type(self.transform.featureType)
        ]
        if self.transform.labelType:
            self.label_fields = [
                f.name for f in _get_fields_from_type(self.transform.labelType)
            ]
        if self.transform.returnType and dataclasses.is_dataclass(
            self.transform.returnType
        ):
            self.return_fields = [
                f.name
                for f in _get_fields_from_type(self.transform.returnType)
            ]
        
    
    # def versionState(self, step, state):
    #     self.state_history[step] = TransformVersion(copy.deepcopy(state), self)
    
    async def fetchFeatures(self, ids, version):
        # Get from datastore
        feature_values = {}
        for id in ids:
            feature_values[id] = self.store.mget(
                id,
                self.feature_fields,
            )

        # Replace from upstream if necessary
        for upstream in self.upstream_executors:
            upstream_feature_names = [
                f for f in upstream.return_fields if f in self.feature_fields
            ]
            # upstream_infers = []
            # with concurrent.futures.ProcessPoolExecutor(10) as executor:
            #     for id in ids:
            #         upstream_infers.append(
            #             asyncio.get_event_loop().run_in_executor(executor, upstream.infer, id, version)
            #         )
            # all_upstream_features = asyncio.get_event_loop().run_until_complete(asyncio.gather(*upstream_infers))
            
            # This def works
            # upstream_infers = [upstream.infer(id, version) for id in ids]
            # all_upstream_features = await asyncio.gather(*upstream_infers)
            # all_upstream_features = {id: f for id, f in zip(ids, all_upstream_features)}
            
            all_upstream_features = await upstream.inferMany(ids, version)
            
            for id, upstream_features in all_upstream_features.items():
                feature_values[id].update(
                    {
                        k: v
                        for k, v in upstream_features.__dict__.items()
                        if k in upstream_feature_names
                    }
                )

        features = [
            self.transform.featureType(
                **{
                    k: v
                    for k, v in feature_values[id].items()
                    if v is not None
                }
            )
            for id in ids
        ]

        return features
    
    async def infer(self, id, version, lazy=False):        
        # If there's a version within eps of version, use that
        closest_version = max(
            [
                v
                for v in self.state_history.keys()
                if v <= version and v > version - self.transform.max_staleness
            ]
            or [None]
        )
        
        if closest_version:
            version = closest_version
        else:
            # Train on all data up to not including the version
            self.state_history[version] = TransformVersion(self, version)
            
        
        # Submit inference
        correct_state = self.state_history[version]
        
        if not lazy:
            res = await correct_state.infer(id)
            return res
        
        # Add to should_process queue
        correct_state.should_process.append(id)
    
    async def inferMany(self, ids, version):
        # If there's a version within eps of version, use that
        closest_version = max(
            [
                v
                for v in self.state_history.keys()
                if v <= version and v > version - self.transform.max_staleness
            ]
            or [None]
        )
        
        if closest_version:
            version = closest_version
        else:
            # Train on all data up to not including the version
            self.state_history[version] = TransformVersion(self, version)
        
        # Submit all inferences
        correct_state = self.state_history[version]
        
        results = await correct_state.inferMany(ids)
        return results
    
    async def processOutstanding(self):
        # Process all outstanding inferences
        all_results = {}
        for _, state in self.state_history.items():
            curr_results = await state.processOutstanding()
            all_results.update(curr_results)
        
        return all_results
            