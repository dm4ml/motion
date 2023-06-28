# State Migrator Tool

Motion provides the [`StateMigrator`](/motion/api/state-migrator/#motion.migrate.StateMigrator). to migrate the state for all instances of a component, given a migration function that takes the old state and returns the new state.

## Usage

The `StateMigrator` is a class that is initialized with a component and migration function. The migration function must have only one argument representing the state of a component instance, and must return a dictionary that replaces the state for that component instance. The migration function is applied to each component instance's state.

This code snippet shows how to use the `StateMigrator` to add a new key to the state of all instances of a component. The migrator is run with a pool of 4 workers.

```python
from motion import Component, StateMigrator

# Create a component with a state with one key
Something = Component("Something")


@Something.init_state
def setup():
    return {"state_val": 0}

# Create a migration function that adds a new key to the state
def my_migrate_func(state):
    state.update({"another_val": 0})
    return state

if __name__ == "__main__":
    # Create a StateMigrator with the component and migration function
    sm = StateMigrator(Something, my_migrate_func)

    # Migrate the state for all instances of the component using a pool of 4 workers
    results = sm.migrate(num_workers=4)

    # See if there were any exceptions
    for result in results:
        if result.exception is not None:
            print(f"Exception for instance {result.instance_id}: {result.exception}")
```

If you want to migrate the state for a single component instance, you can pass in the instance IDs of the component instances to migrate:

```python
results = sm.migrate(instance_ids=[...], num_workers=4)
```
