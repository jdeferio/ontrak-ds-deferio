"""### Data Scientist Technical Assessment

1. Identify population that graduated on or after 1/1/2019
2. Find the correct enrolled date for each member that had graduated on or after 1/1/2019 
    using the following business logic:
    - Any member that has an enrollment date.
    - Enrollment date will be the first date of enrollment as entered for a member unless:
        * Member has graduated and reenrolled:
            * Then use the enrollment date following the latest graduation date
        * Member has disenrolled and reenrolled >180 days _after_ disenrollment
            * Then use the enrollment date following the disenrollment date
3. The final output should be member_id, payor, enrollment_date and graduation_date for the 
    graduated population on or after 1/1/2019


__Assumptions__
* A member can be enrolled in several different Health Plans (payor) at any given point, but 
    this has no obvious impact on Ontrak enrollment for the purposes of this exercise.
* Requested payor is in the context of the targeted enrollement date
* Number of graduations is irrelevant, therefore there will be 1 target enrollment date per 
    graduated member (based on the provided business logic and question parameters).
"""

import datetime as dt

import pandas as pd

rawdata = pd.read_excel(
    "Data Scientist Technical Excercise.xlsx",
    sheet_name="data",
    dtype={"fake_id": str},
    parse_dates=["StatusDate"],
)


class Solution:
    def __init__(self, rdata):
        self.rdata = rdata
        self.TO_GRAD = 365
        self.TO_RESET = 180

    def process_raw(self):
        """Ingests raw dataframe and performs the following functions:

            1. Recode the Status column to int (0:enroll, 1:disenroll, 2:graduate)
            2. Sort dataframe by fake_id (member_id), StatusDate, and Status -> ascending
            3. Drop rows with missing dates and standardize values to datetime64

        Returns: Processed DataFrame (data_sorted)
        """
        encode = {
            "Enrolled": 0,
            "Disenrolled": 1,
            "Graduated": 2,
        }
        self.rdata["eventStatus"] = [encode.get(x) for x in self.rdata["Status"]]
        data_sorted = self.rdata.sort_values(
            by=["fake_id", "StatusDate", "eventStatus"], ascending=[True, True, True]
        )

        self.df = data_sorted.dropna(subset=["StatusDate"], axis=0)
        dateval = lambda x: pd.to_datetime(x, format="%Y-%m-%d")
        self.df["StatusDate"] = self.df["StatusDate"].apply(dateval)

    def find_graduated(self):
        """Ingests the processed DataFrame to identify members whom had
        graduated on or after 2019-01-01.

        Returns: DataFrame containing all events for graduated members (data_grad_sort)
        """

        ix_status = self.df["Status"] == "Graduated"
        ix_date = self.df["StatusDate"] >= pd.to_datetime(
            "2019-01-01", format="%Y-%m-%d"
        )

        grads = self.df.loc[ix_status & ix_date]
        self.id_list = sorted(list(set(grads["fake_id"])))

    def find_enrollment(self):
        """For all graduated members, this function will find the appropriate
        enrollment date given business logic:

        Enrollment date will be the first date of enrollment as entered for a member unless:
            * Member has graduated and reenrolled:
                * Then use the enrollment date following the latest graduation date
            * Member has disenrolled and reenrolled >180 days after disenrollment
                * Then use the enrollment date following the disenrollment date

        Returns: Response DataFrame containing member id, payor, enrollment date, grad date and health plan
        """

        cols = [
            "member_id",
            "payor",
            "enroll_date",
            "graduation_date",
            "short_grad_time",
        ]
        self.response = pd.DataFrame(columns=cols)

        # iterate over each graduated member
        for member in self.id_list:
            ix_id = self.df["fake_id"] == member
            tmp_df = self.df.loc[ix_id]  # filter for member_id ('fake_id')

            tmp_df = tmp_df.sort_values(
                by=["StatusDate", "eventStatus"], ascending=[True, True]
            ).copy()

            tmp_df_tuples = list(tmp_df.itertuples(index=False))

            # establish a hash dict to remember values
            seen = {}
            seen["member_id"] = member
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
                    if (i.StatusDate - seen["disenroll_date"]).days > self.TO_RESET:
                        seen["enroll_date"] = i.StatusDate
                        continue

                # record graduation dates, check if graduation meets time criteria
                if i.Status == "Graduated":
                    seen["graduation_date"] = i.StatusDate
                    if (
                        seen["graduation_date"] - seen["enroll_date"]
                    ).days >= self.TO_GRAD:
                        continue

                    if "enroll_date2" in seen:
                        if (
                            seen["graduation_date"] - seen["enroll_date2"]
                        ).days >= self.TO_GRAD:
                            seen["enroll_date"] = seen["enroll_date2"]
                            seen["payor"] = seen["payor2"]
                            continue

                    if (
                        seen["graduation_date"] - seen["enroll_date"]
                    ).days < self.TO_GRAD:
                        seen["short_grad_time"] = 1

            seen_df = pd.DataFrame(seen, index=[0])
            seen_df = seen_df.loc[:, cols]

            self.response = pd.concat(
                [self.response, seen_df], ignore_index=True, axis=0, sort=False
            )
        
        # output result to excel document
        tmstp = dt.datetime.today().strftime(format="%Y%m%d")
        self.response.to_excel(
            f"ontrak_ds_deferio_{tmstp}.xlsx",
            sheet_name="Result",
            encoding="utf-8",
            index=False,
        )


sol = Solution(rawdata)
sol.process_raw()
sol.find_graduated()
sol.find_enrollment()
