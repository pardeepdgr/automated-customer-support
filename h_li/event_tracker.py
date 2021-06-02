from h_li.client.event_reader import read_tracking_extract
from h_li.filter.event_cleanser import apply_filters
from h_li.insight.event_analyzer import generate_report

if __name__ == "__main__":
    generate_report(
        apply_filters(
            read_tracking_extract()
        )
    )
