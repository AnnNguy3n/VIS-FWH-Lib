import pandas as pd


filter_fields = {
    "DoubleYearThreshold": {
        "fields": "ValGeo2;GeoNgn2;ValHar2;HarNgn2",
        "command": "DYT_filter_"
    },
}


generate_method = {
    "HomogeneousPolynomial": {
        "command": "HP_method_"
    },
}


def compare_dfs(df1: pd.DataFrame, df2: pd.DataFrame, tolerance = 0.0):
    assert df1.shape == df2.shape, f"Mismatch in shape: {df1.shape} vs {df2.shape}"

    for col in df1.columns:
        assert col in df2.columns, f"Column '{col}' not found in df2"
        print(f"Comparing column: {col}")

        dtype1, dtype2 = df1[col].dtype, df2[col].dtype
        assert dtype1 == dtype2, f"Mismatch in dtype for column '{col}': {dtype1} vs {dtype2}"

        if dtype1 == "object":
            mismatches = (df1[col] != df2[col]).sum()
            assert mismatches == 0, f"Column '{col}' has {mismatches} mismatches"

        elif dtype1 in ["int64", "float64"]:
            max_diff = (df1[col] - df2[col]).abs().max()
            assert max_diff <= tolerance, f"Column '{col}' has max difference {max_diff}, exceeds tolerance {tolerance}"

        else:
            raise TypeError(f"Unsupported data type in column '{col}': {dtype1}")
