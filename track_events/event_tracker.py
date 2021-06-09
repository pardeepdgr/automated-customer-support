from track_events.client.event_reader import read_tracking_extract
from track_events.filter.event_cleanser import apply_filters
from track_events.insight.event_analyzer import generate_report

if __name__ == "__main__":
    generate_report(
        apply_filters(
            read_tracking_extract()
        )
    )
