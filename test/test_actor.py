import pytest
from src.model.ActorModel import Actor

@pytest.fixture
def actor():
    return Actor(
        rank=1,
        year=2000,
        gdppc=60000,
        neighbor="{'name': 'Austria', 'direction': 'E'}"
        )


def test_actor_year_must_be_greater_than_1900(actor:Actor):
    assert actor.year > 1900

def test_actor_neighbor_must_not_be_null(actor:Actor):
    assert actor.neighbor is not None