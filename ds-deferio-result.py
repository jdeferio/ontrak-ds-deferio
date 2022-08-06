import uuid

import pandas as pd

TO_GRAD = 365
TO_RESET = 180

rawdata = pd.read_excel(
    "Data Scientist Technical Excercise.xlsx",
    sheet_name="data",
    dtype={"fake_id": str},
    parse_dates=["StatusDate"],
)


def process_raw(data):
    """Ingests raw dataframe and performs the following functions:

        1. Assign unique ID associated with each event
        2. Recode the Status column to int (0:enroll, 1:disenroll, 2:graduate)
        2. Sort dataframe by fake_id (member_id), StatusDate, and Status -> ascending
        3. Drop rows with missing dates and standardize values to datetime64

    Returns: Processed DataFrame (data_sorted)
    """
    encode = {
        "Enrolled": 0,
        "Disenrolled": 1,
        "Graduated": 2,
    }
    data["eventID"] = [uuid.uuid4() for i in range(len(data))]
    data["eventStatus"] = [encode.get(x) for x in data["Status"]]
    data_sorted = data.sort_values(
        by=["fake_id", "StatusDate", "eventStatus"], ascending=[True, True, True]
    )

    data_sorted_clean = data_sorted.dropna(subset=["StatusDate"], axis=0)
    dateval = lambda x: pd.to_datetime(x, format="%Y-%m-%d")
    data_sorted_clean["StatusDate"] = data_sorted_clean["StatusDate"].apply(dateval)

    return data_sorted_clean


def find_graduated(data):
    """Ingests the processed df to identify members whom had graduated on or
    after 2019-01-01.

    Returns: DataFrame containing all events for graduated members (data_grad_sort)
    """
    datacopy = data.copy()

    ix_status = datacopy["Status"] == "Graduated"
    ix_date = datacopy["StatusDate"] >= pd.to_datetime("2019-01-01", format="%Y-%m-%d")

    grads = datacopy.loc[ix_status & ix_date]
    grad_ids = set(grads["fake_id"].tolist())

    datacopy["is_grad"] = datacopy["fake_id"].apply(lambda x: 1 if x in grad_ids else 0)

    data_grad = datacopy.loc[datacopy["is_grad"] == 1]
    data_grad = data_grad.sort_values(
        by=["fake_id", "StatusDate", "eventStatus"], ascending=[True, True, True]
    )
    data_grad.drop(columns=["is_grad"], inplace=True)

    return data_grad


def find_enrollment(data2):
    """For all graduated members, this function will find the appropriate
    enrollment date given business logic:

    Enrollment date will be the first date of enrollment as entered for a member unless:
        * Member has graduated and reenrolled:
            * Then use the enrollment date following the latest graduation date
        * Member has disenrolled and reenrolled >180 days after disenrollment
            * Then use the enrollment date following the disenrollment date

    Returns: Response DataFrame containing member id, payor, enrollment date, grad date and health plan
    """

    cols = ["member_id", "payor", "enroll_date", "graduation_date", "short_grad_time"]
    response = pd.DataFrame(columns=cols)
    grad_id_list = list(set(data2.fake_id))

    # iterate over each graduated member
    for grad_id in grad_id_list:
        ix_id = data2["fake_id"] == grad_id
        tmp_df = data2.loc[ix_id]  # filter for member_id ('fake_id')

        tmp_df = tmp_df.sort_values(
            by=["StatusDate", "eventStatus"], ascending=[True, True]
        ).copy()

        tmp_df_tuples = list(tmp_df.itertuples(index=False))

        # establish a hash dict to remember values
        seen = {}
        seen["member_id"] = grad_id
        seen["short_grad_time"] = 0

        for i in tmp_df_tuples:
            # record earliest enrollment and payor
            if i.Status == "Enrolled" and "enroll_date" not in seen:
                seen["enroll_date"] = i.StatusDate
                seen["payor"] = i.fake_health_plan
                continue

            # record disenrollment
            if i.Status == "Disenrolled":
                seen["disenroll_date"] = i.StatusDate
                continue

            # account for multiple enrollment dates and graduation
            if i.Status == "Enrolled" and "enroll_date" in seen:
                if "graduation_date" in seen:
                    seen["enroll_date"] = i.StatusDate
                    seen["payor"] = i.fake_health_plan
                    continue

                if "disenroll_date" not in seen:
                    seen["enroll_date2"] = i.StatusDate
                    seen["payor2"] = i.fake_health_plan
                    continue
                if (i.StatusDate - seen["disenroll_date"]).days > TO_RESET:
                    seen["enroll_date"] = i.StatusDate
                    continue

            # record graduation dates, check if graduation meets time criteria
            if i.Status == "Graduated":
                seen["graduation_date"] = i.StatusDate
                if (seen["graduation_date"] - seen["enroll_date"]).days >= TO_GRAD:
                    continue

                if "enroll_date2" in seen:
                    if (seen["graduation_date"] - seen["enroll_date2"]).days >= TO_GRAD:
                        seen["enroll_date"] = seen["enroll_date2"]
                        seen["payor"] = seen["payor2"]
                        continue

                if (seen["graduation_date"] - seen["enroll_date"]).days < TO_GRAD:
                    seen["short_grad_time"] = 1

        seen_df = pd.DataFrame(seen, index=[0])
        seen_df = seen_df.loc[:, cols]
        response = pd.concat([response, seen_df], ignore_index=True, axis=0, sort=False)

    return response


df = process_raw(rawdata)
df_grad = find_graduated(df)
result = find_enrollment(df_grad)

result.to_csv('ontrak_ds_deferio_result.csv', encoding='utf-8', index=False)