from motion import Component, MDataFrame

C = Component("MyDFComponent")


@C.init_state
def setUp():
    return {"df": MDataFrame({"value": [0, 1, 2]})}


def test_read_and_write_to_df():
    c_instance = C()
    df = c_instance.read_state("df")
    assert df.to_dict(orient="list") == {"value": [0, 1, 2]}

    # Add a column
    df["value2"] = [3, 4, 5]
    c_instance.write_state({"df": df})

    # Check that the column was added
    df_2 = c_instance.read_state("df")
    assert df_2.to_dict(orient="list") == {
        "value": [0, 1, 2],
        "value2": [3, 4, 5],
    }
