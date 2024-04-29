
import pytest

from lit_stud.utils.keywords import KeywordGroups, KeywordGroup


@pytest.fixture
def groups():
    groups = KeywordGroups()
    groups.append(KeywordGroup("sim", ["simulating", "simulator", "simulate"], weight=1))
    groups.append(KeywordGroup("model", ["model", "modeling"], weight=0.5))
    return groups

def test_group_print(groups):
    print()
    print(groups)


def test_group1(groups):
    text = "Hello, this is a text"
    sum = groups.evaluate_keywords(text)

    assert sum == 0

def test_group2(groups):
    text = "Hello, this is a text, simulating"
    sum = groups.evaluate_keywords(text)

    assert sum == 1

def test_group3(groups):
    text = "Hello, this is a text, simulating, simulate, \nmodel, model"
    sum = groups.evaluate_keywords(text)

    assert sum == 1.5