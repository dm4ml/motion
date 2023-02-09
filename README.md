# motion

To dos:

- [x] Handle dependent models
- [x] Profile code for performance
- [x] Parallelize inference (only done via multithreading though, which is kind of useless)
- [ ] Create abstract batch inference method in the transform class
- [ ] Create custom feature type dataclass with dict methods and cheap serialization methods
- [ ] Cache features when we have dependent models
- [ ] Implement buffer for each transform executor
- [ ] Track label provenance and make sure there is no leakage between features and label