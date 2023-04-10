# Connecting to a Motion Application in Javascript

The Motion API is a RESTful API that can be used to connect to a Motion application from any programming language. This tutorial will show you how to connect to a Motion application from Javascript. See the [HTTP API](/api/http) documentation for more information.

## Set Up Cooking App

We'll be using the example Cooking app from the [Starter](/tutorials/recipe/starter) tutorial. Create a cooking project:

```bash
$ motion example
Example application name: cooking
Your name: shreya
Created a project successfully.
```

Make sure your Motion API token is set, by following the [configuration instructions](/getting_started/configuration). Then navigate to the project directory and start the app.

```bash
$ cd cooking
$ motion serve
```

If you get an error saying the default port is already in use, you can specify a different port with the `--port` flag.

```bash
$ motion serve --port=5001
INFO:     Started server process [21067]
INFO:     Waiting for application startup.
INFO:     Application startup complete.
INFO:     Uvicorn running on http://0.0.0.0:5001 (Press CTRL+C to quit)
```

The app should now be running.

## Make an End-to-End Query

We'll use the `axios` library to make HTTP requests to the Motion API. Install it with `npm`:

```bash
npm install axios
```

Then create a new Javascript file, `query.js`, and add the following code:

```javascript
var axios = require('axios');

const token = process.env.MOTION_API_TOKEN;
const headers = {
    'Content-Type': 'application/json',
    'Authorization': `Bearer ${token}` // Replace with your MOTION_API_TOKEN if you don't have it set as an environment variable
};
```

We'll now try to get similar recipes to a list of ingredients given in the query, similar to the `test.py` script in the [starter tutorial](/tutorials/recipe/starter/#testing). There are 3 Motion endpoints that we must use:

1. `POST /json/set/` to set new ingredients (the user's query), which runs relevant Motion triggers to compute similar recipes based on embeddings
2. `GET /json/get/` to retrieve the similar recipe IDs
3. `GET /json/mget/` to retrieve the recipe names and URLs for the similar recipe IDs

The full code for the query is:

```javascript title="query.js"
var axios = require('axios');

const token = process.env.MOTION_API_TOKEN;
const headers = {
    'Content-Type': 'application/json',
    'Authorization': `Bearer ${token}` // Replace with your MOTION_API_TOKEN if you don't have it set as an environment variable
};

(async () => { // Use async/await to make HTTP requests
    var setConfig = {
        method: 'post',
        url: 'http://127.0.0.1:5001/json/set/', // Replace with your app's URL
        headers: headers,
        data: JSON.stringify({
            "relation": "Query",
            "identifier": "",
            "key_values": {
                "ingredients": "pasta;tomatoes;garlic;cheese"
            }
        })
    };

    const response = await axios(setConfig);
    identifier = response.data;

    // Make get call for recipe ids
    var getConfig = {
        method: 'get',
        url: 'http://127.0.0.1:5001/json/get/', // Replace with your app's URL
        headers: headers,
        data: JSON.stringify({
            "relation": "Query",
            "identifier": identifier,
            "keys": ["identifier", "recipe_id"],
            "include_derived": true
        })
    };

    const getResponse = await axios(getConfig);

    // Make mget call for recipe names and urls

    var mgetConfig = {
        method: 'get',
        url: 'http://127.0.0.1:5001/json/mget/', // Replace with your app's URL
        headers: headers,
        data: JSON.stringify({
            "relation": "Recipe",
            "identifiers": getResponse.data.map((item) => item.recipe_id),
            "keys": ["title", "recipe_url"]
        })
    };
    const mgetResponse = await axios(mgetConfig);
    console.log(mgetResponse.data);

})();
```

Then, if you run the script with `node query.js`, you should see the following output:

```bash
$ node query.js
[
  {
    title: 'Best Eggplant Parmesan',
    recipe_url: 'https://www.bonappetit.com/recipe/bas-best-eggplant-parmesan',
    identifier: '9588abca-e8a4-4491-b9e0-3c1b55f3d9e7'
  },
  {
    title: "BA's Best Baked Ziti",
    recipe_url: 'https://www.bonappetit.com/recipe/baked-ziti',
    identifier: 'ee36b8fc-626b-401c-bfaa-cfa865545aa1'
  },
  {
    title: 'Baked Tomato Feta Pasta With a Kick',
    recipe_url: 'https://www.bonappetit.com/recipe/spicy-feta-pasta-recipe',
    identifier: 'c596df3c-fb6c-4d3d-9342-a075c61e00aa'
  },
  {
    title: 'Grated Tomato and Miso-Butter Pasta',
    recipe_url: 'https://www.bonappetit.com/recipe/grated-tomato-and-miso-butter-pasta-recipe',
    identifier: '669a6642-7738-4553-9740-a42d90dac928'
  },
  ... 6 more items
]
```