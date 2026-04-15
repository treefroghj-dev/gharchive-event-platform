{% test distinct_event_dates_cover_past_days(model, days=14, date_column="event_ts") %}
    {# Each source must have at least `days` distinct calendar dates from date(date_column) in [anchor - days, anchor); anchor day excluded. #}
    with covered_dates as (
        select distinct date({{ date_column }}) as event_date
        from {{ model }}
        where
            date({{ date_column }}) >= date_sub({{ anchor_date() }}, interval {{ days | int }} day)
            and date({{ date_column }}) < {{ anchor_date() }}
    ),
    date_coverage as (
        select count(*) as distinct_date_count
        from covered_dates
    )
    select distinct_date_count
    from date_coverage
    where distinct_date_count < {{ days | int }}
{% endtest %}
