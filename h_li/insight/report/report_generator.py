import json
from typing import List

from pyspark import Row
from pyspark.sql import DataFrame


def create_report(analysis_1: DataFrame, analysis_2: int, analysis_3: List[str], analysis_3a: int,
                  analysis_3b: int, analysis_4: int, analysis_5: int, analysis_6: int,
                  analysis_7a: int, analysis_7b: int, analysis_8: int):
    rows: List[Row] = analysis_1.collect()

    report = {"analysis-1": dict(zip([str(row.device_class) for row in rows], [str(row.device_count) for row in rows])),
              "analysis-2": {"users": analysis_2},
              "analysis-3": {"users_session_less_than_5sec": len(analysis_3),
                             "user_journeys_started_with_search_page": analysis_3a,
                             "user_journeys_ended_with_apartment_view": analysis_3b},
              "analysis-4": {"users": analysis_4},
              "analysis-5": {"users": analysis_5},
              "analysis-6": {"users": analysis_6},
              "analysis-7": {"user_with_filter_above_1200_EUR": analysis_7a,
                             "user_with_filter_below_1200_EUR": analysis_7b},
              "analysis-8": {"users": analysis_8},
              }

    insights = json.dumps(report, indent=4)

    with open("insights.json", "w") as outfile:
        outfile.write(insights)
