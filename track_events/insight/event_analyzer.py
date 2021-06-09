from track_events.insight.helper.dataframe_analyzer import *
from track_events.insight.helper.json_analyzer import *
from track_events.insight.report.report_generator import create_report


def generate_report(data: DataFrame):
    analysis_1: DataFrame = count_by_devices(data)

    analysis_2: int = count_user_sessions_first_started_with_an_om_source(data)

    # analysis-3
    users: List[str] = user_sessions_duration_less_than(5, data)
    analysis_3a: int = user_journeys_started_with("search_page", users, data)
    analysis_3b: int = user_journeys_ended_with("apartment_view", users, data)

    analysis_4: int = user_journeys_started_on_search_page_later_visited_apartment_view(data)

    analysis_5: int = users_clicked_book_now(data)

    analysis_6: int = users_started_with_search_page_and_executed_request_draft_created(data)

    # analysis-7
    users_with_euro_filter: DataFrame = users_set_price_filter_in_euro(data)
    users_with_euro_filter.persist()
    analysis_7a: int = users_set_price_filter_above(1200, users_with_euro_filter)
    analysis_7b: int = users_set_price_filter_below(1200, users_with_euro_filter)

    analysis_8: int = user_session_contains_request_id(data)

    create_report(analysis_1, analysis_2, users, analysis_3a, analysis_3b, analysis_4,
                  analysis_5, analysis_6, analysis_7a, analysis_7b, analysis_8)
