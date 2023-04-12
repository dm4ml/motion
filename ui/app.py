from flask import Flask, jsonify

app = Flask(__name__)

items = [{"id": 1, "name": "Apples"}, {"id": 2, "name": "Oranges"}]


@app.route("/items")
def get_items():
    return jsonify(items)


@app.route("/item/<int:id>")
def get_item(id):
    item = [i for i in items if i["id"] == id]
    return jsonify(item[0])


if __name__ == "__main__":
    app.run(debug=True)
