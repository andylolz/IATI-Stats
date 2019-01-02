from lxml import etree

from collections import defaultdict, OrderedDict
from stats.common.decorators import *

def test_budget_not_provided_works():
    activity_stats = ActivityStats()
    activity_stats.element = etree.fromstring('''
            <iati-activity budget-not-provided="1">
            </iati-activity>
    ''')
    assert activity_stats._budget_not_provided() == {'budget-not-provided': '1'}

def test_budget_not_provided_fails():
    activity_stats = ActivityStats()
    activity_stats.element = etree.fromstring('''
            <iati-activity>
            </iati-activity>
    ''')
    assert activity_stats._budget_not_provided() == {'budget-not-provided': '0'}

def test_budget_not_provided_value():
    activity_stats = ActivityStats()
    activity_stats.element = etree.fromstring('''
            <iati-activity budget-not-provided="1">
            </iati-activity>
    ''')
    assert int(activity_stats._budget_not_provided().values()[0]) == 1


class CommonSharedElements(object):
    blank = False


class ActivityStats(CommonSharedElements):
    """ Stats calculated on a single iati-activity. """
    element = None
    blank = False
    strict = False # (Setting this to true will ignore values that don't follow the schema)
    context = ''
    comprehensiveness_current_activity_status = None
    now = datetime.datetime.now()

    def _budget_not_provided(self):
        if self.element.attrib.get('budget-not-provided'):
            return {'budget-not-provided': self.element.attrib.get('budget-not-provided')}
        else:
            return {'budget-not-provided': '0'}