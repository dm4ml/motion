# motion

To dos:

- [x] Handle dependent models
- [x] Profile code for performance
- [x] Parallelize inference (only done via multithreading though, which is kind of useless)
- [x] Create abstract batch inference method in the transform class
- [ ] Create custom feature type dataclass with dict methods and cheap serialization methods
- [x] Cache features when we have dependent models
- [x] Implement buffer for each transform executor
- [ ] Auto retrain
- [ ] Implement `inc()`
- [ ] Incorporate feedback / do evaluation
- [ ] Track label provenance and make sure there is no leakage between features and label
- [ ] Pinecone integration

## Interesting apps to reproduce

- [Transcript search/summarization](https://www.markiewagner.com/summ)