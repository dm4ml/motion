# Testing Plan

## Unit Tests

- [PASS] Create component with `infer` and `fit` routes for 1 key
- [PASS] Create component with `infer` and `fit` routes for 2 keys
- [PASS] Create component with `infer` route for 1 key and `fit` route for 2 keys
- [PASS] Create component with `infer` route for 2 keys and `fit` route for 1 key
- [FAIL] Create component with 2 `infer` routes for 1 key
- [FAIL] Create component with 2 `fit` routes for 1 key
- [PASS] `fit` route with batch_size=1, 10, 100
- [FAIL] Run for key with no routes
- [FAIL] Run for multiple keys
- [PASS] Run for key with routes
- [PASS] Run for key with fit route, wait_for_fit=True
- [FAIL] Run for key with infer route, wait_for_fit=True
- [PASS] Run for key with fit route, return_fit_event=True

## Integration Tests

- [PASS] DB connection in state
- [PASS] ML model in state
- [PASS] A pipeline with 2 components
