
import pytest
from helpers.utils import is_same
from usage_report.usage_report import agg_usage, get_spark
from pyspark.sql import Row
from usage_report.utils.trackingprotection import pct_etp


#  Makes utils available
pytest.register_assert_rewrite('tests.helpers.utils')


@pytest.fixture
def spark():
    return get_spark()


@pytest.fixture
def main_summary_data():
    a1 = [Row(addon_id=u'disableSHA1rollout', name=u'SHA-1 deprecation staged rollout',
              foreign_install=False, is_system=False),
          Row(addon_id=u'e10srollout@mozilla.org', name=u'Multi-process staged rollout',
              foreign_install=False, is_system=True)]

    a2 = [Row(addon_id=u'disableSHA1rollout', name=u'SHA-1 deprecation staged rollout',
              foreign_install=False, is_system=False),
          Row(addon_id=u'e10srollout@mozilla.org', name=u'Multi-process staged rollout',
              foreign_install=False, is_system=True)]

    return (
        (("20180201", 100, 20, "US", "client1", "57.0.1", 17060,
          "Windows_NT", 10.0, a1, {0: 0, 1: 1}, {3: 0, 4: 1}, 'en-US'),
         ("20180201", 100, 20, "US", "client2", "57.0.1", 17060,
          "Windows_NT", 10.0, a1, {0: 0, 1: 1}, {2: 0, 3: 1}, 'en-US'),
         ("20180201", 100, 20, "DE", "client3", "57.0.1", 17060,
          "Windows_NT", 10.0, a1, None, None, 'DE'),
         ("20180201", 100, 20, "DE", "client3", "57.0.1", 17060,
          "Windows_NT", 10.0, a1, {}, {}, "DE"),
         ("20180201", 100, 20, "DE", "client4", "58.0", 17563,
          "Darwin", 10.0, a2, None, None, "DE")),  # 17563 -> 20180201
        ["submission_date_s3", "subsession_length", "active_ticks",
         "country", "client_id", "app_version", "profile_creation_date",
         "os", "os_version", "active_addons", "histogram_parent_tracking_protection_enabled",
         "histogram_parent_cookie_behavior", "locale"]
    )


def test_pct_etp_no_country_list_pt2(spark, main_summary_data):
    main_summary = spark.createDataFrame(*main_summary_data)
    without_country_list = pct_etp(main_summary, '20180201')

    expected = [
        {
            "submission_date_s3": "20180201",
            "country": "All",
            "pct_ETP": 25.0
        }
    ]

    is_same(spark, without_country_list, expected)


def test_pct_etp_country_list_pt2(spark, main_summary_data):
    main_summary = spark.createDataFrame(*main_summary_data)
    with_country_list = pct_etp(main_summary,
                                '20180201',
                                country_list=["DE"])
    expected = [
        {
            "submission_date_s3": "20180201",
            "country": "All",
            "pct_ETP": 25.0
        },
        {
            "submission_date_s3": "20180201",
            "country": "DE",
            "pct_ETP": 0.0
        }
    ]

    is_same(spark, with_country_list, expected)
