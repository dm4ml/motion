# Component Instance Clearing

## CLI Documentation

To easily clear a component instance, you can use the CLI command `motion clear`. If you type `motion clear --help`, you will see the following:

```bash
$ motion clear --help
Usage: motion clear [OPTIONS] INSTANCE

  Clears the state and cached results for a component instance.

  Args:     instance (str): Instance name of the component to clear.
  In the form `componentname__instancename`.

Options:
  --help  Show this message and exit.

  Example usage: motion clear MyComponent__myinstance
```

## Python Documentation

::: motion.utils.clear_instance
    handler: python
    options:
        show_root_full_path: false
        show_root_toc_entry: false
        show_root_heading: true
        show_source: false
        show_signature_annotations: true
        heading_level: 3
