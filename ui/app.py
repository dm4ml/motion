from flask import Flask, render_template, jsonify, request

app = Flask(__name__, static_folder="public")


@app.route("/names")
def names():
    return jsonify(["Alice", "Bob", "Charlie", "David", "Emily", "Frank"])


@app.route("/timeline")
def timeline():
    query = request.args.get("query")
    if query:
        return jsonify(
            [f"{query} result 1", f"{query} result 2", f"{query} result 3"]
        )
    else:
        return jsonify([])


@app.route("/")
def index():
    return render_template("index.html")


if __name__ == "__main__":
    app.run()
