# Configuring Environment Variables

Motion requires any connection request to a Motion application to be authenticated. This is achieved by configuring a `MOTION_API_TOKEN` for your application, which should be an environment variable on the server running your application.

You can either choose your own key or create a random token by running this in your terminal:

```bash
motion token
```

Which will return some token, e.g., `f1991cbb929bc38d5cffe9137aa85ca989a94803`. Then you can set your `MOTION_API_TOKEN` to the output (or a key of your choice):

```bash
export MOTION_API_TOKEN=f1991cbb929bc38d5cffe9137aa85ca989a94803
```

The authentication is necessary because Motion applications are often deployed on remote servers.