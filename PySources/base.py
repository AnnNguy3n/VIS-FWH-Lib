import pandas as pd
import numpy as np
import numba as nb


def check_required_cols(data: pd.DataFrame, required_cols: set | list):
    required_cols = set(required_cols)
    missing_cols = required_cols - set(data.columns)
    assert not missing_cols, missing_cols

def check_dtypes(data: pd.DataFrame, expected_dtypes: dict):
    for col, dtype in expected_dtypes.items():
        if isinstance(dtype, list):
            assert data[col].dtype in dtype, f"{col}'s dtype must be one of {dtype}"
        else:
            assert data[col].dtype == dtype, f"{col}'s dtype must be {dtype}"


class Base:
    def __init__(
            self,
            data: pd.DataFrame,
            interest: float,
            valuearg_threshold: float
    ) -> None:
        data = data.reset_index(drop=True).fillna(0.0)

        # Các cột bắt buộc
        self.drop_cols = {"TIME", "PROFIT", "SYMBOL", "VALUEARG"}
        check_required_cols(data, self.drop_cols)

        # Check dtypes
        check_dtypes(data, {
            "TIME": "int64",
            "PROFIT": "float64",
            "VALUEARG": ["int64", "float64"]
        })

        # Cột TIME phải đơn điệu giảm
        assert data["TIME"].is_monotonic_decreasing, "Cột TIME không đơn điệu giảm"

        # Cột PROFIT và VALUEARG phải không âm
        for col in ["PROFIT", "VALUEARG"]:
            assert data[col].min() >= 0.0, f"{col} < 0.0"

        # Kiểm tra chu kỳ thiếu và lập chỉ mục
        unique_time = np.arange(data["TIME"].max(), data["TIME"].min() - 1, -1)
        assert np.array_equal(unique_time, data["TIME"].unique()), "Thiếu chu kỳ trong TIME"

        self.INDEX = np.searchsorted(-data["TIME"], -unique_time, side="left")
        self.INDEX = np.append(self.INDEX, data.shape[0])

        # Kiểm tra sự unique của SYMBOL trong mỗi chu kỳ
        assert np.array_equal(
            data.groupby("TIME", sort=False)["SYMBOL"].nunique().values,
            np.diff(self.INDEX)
        ), "SYMBOL không unique ở mỗi chu kỳ"

        # Loại những cột không có kiểu dữ liệu là int64 hoặc float64
        self.drop_cols.update(data.select_dtypes(exclude=["int64", "float64"]).columns)
        print("Các cột không được coi là biến chạy:", self.drop_cols)

        # Attrs
        self.data = data
        self.INTEREST = interest

        # Chuyển PROFIT và VALUEARG sang NumPy, đảm bảo PROFIT không nhỏ hơn 5e-324
        self.PROFIT = np.maximum(data["PROFIT"].to_numpy(dtype=np.float64), 5e-324)
        self.VALUEARG = data["VALUEARG"].to_numpy(dtype=np.float64)
        self.BOOL_ARG = self.VALUEARG >= valuearg_threshold

        # Mã hóa SYMBOL thành số nguyên
        unique_symbols, self.SYMBOL = np.unique(data["SYMBOL"], return_inverse=True)
        self.symbol_name = dict(enumerate(unique_symbols))

        # Xử lý biến toán hạng (OPERAND)
        operand_data = data.drop(columns=self.drop_cols)
        self.operand_name = dict(enumerate(operand_data.columns))
        self.OPERAND = operand_data.to_numpy(dtype=np.float64).T


@nb.njit
def calculate_formula_Poly(formula, operand):
    f_size = formula.size
    temp_0 = np.zeros(operand.shape[1], dtype=np.float64)
    temp_1 = np.empty(operand.shape[1], dtype=np.float64)
    temp_op = -1

    for i in range(1, f_size, 2):
        op_code = formula[i-1]
        op_index = formula[i]
        if op_code < 2:
            temp_op = op_code
            temp_1[:] = operand[op_index]
        else:
            if op_code == 2:
                temp_1[:] *= operand[op_index]
            else:
                temp_1[:] /= operand[op_index]

        if i+1 == f_size or formula[i+1] < 2:
            if temp_op == 0:
                temp_0[:] += temp_1
            else:
                temp_0[:] -= temp_1

    temp_0[np.isnan(temp_0) | np.isinf(temp_0)] = -1.7976931348623157e+308
    return temp_0


@nb.njit
def decode_formula(f, len_):
    rs = np.zeros(f.size*2, dtype=np.int64)
    rs[0::2] = f // len_
    rs[1::2] = f % len_
    return rs


@nb.njit
def encode_formula(f, len_):
    return f[0::2] * len_ + f[1::2]


__STRING_OPERATOR = "+-*/"

@nb.njit
def convert_arrF_to_strF(arrF):
    strF_list = []
    for i in range(arrF.size):
        if i % 2 == 1:
            strF_list.append(str(arrF[i]))
        else:
            strF_list.append(__STRING_OPERATOR[arrF[i]])
    return "".join(strF_list)


@nb.njit
def convert_strF_to_arrF(strF):
    f_len = 0
    for c in strF:
        f_len += c in __STRING_OPERATOR
    arrF = np.zeros(f_len*2, dtype=np.int64)
    len_strF = len(strF)
    idx, arr_idx = 0, 0

    while idx < len_strF:
        if strF[idx] in __STRING_OPERATOR:
            arrF[arr_idx] = __STRING_OPERATOR.index(strF[idx])
            arr_idx += 1
            idx += 1
        else:
            while idx < len_strF and strF[idx] not in __STRING_OPERATOR:
                arrF[arr_idx] = 10 * arrF[arr_idx] + ord(strF[idx]) - ord("0")
                idx += 1
            arr_idx += 1
    return arrF
