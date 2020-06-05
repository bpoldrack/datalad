"""utilities to assist with setting up tests"""

from datalad.core.distributed.clone import (
    Clone,
    decode_source_spec
)
from datalad.distribution.dataset import Dataset
from datalad.interface.common_cfg import dirs
from datalad.utils import (
    better_wraps,
    optional_args,
    Path
)
from datalad.tests.utils import with_tempfile


DATALAD_TESTS_CACHE = Path(dirs.user_cache_dir) / "tests"


def get_cached_dataset(url, dataset_name=None, version=None):
    """ Helper to get a cached clone from url

    Intended for use from within `datalad_dataset` decorator.
    Clones `url` into user's cache under datalad/tests/`name`. If such a clone
    already exists, don't clone but return the existing one. So, it's supposed
    to cache the original source in order to reduce time and traffic for tests,
    by letting subsequent requests clone from a local location directly.

    If it's an annex get the content, too.

    Note
    ----
    `version` not yet implemented. Intention: verify that `version` can be
    checked out, but don't actually do it, since the cached dataset is intended
    to be used as origin instead of the original remote at URL by the
    `datalad_dataset` test decorator. Checkout of a particular should happen

    Parameters
    ----------
    url: str
        URL to clone from
    dataset_name: str
        (directory) name to use for the clone

    Returns
    -------
    Dataset
    """

    # TODO: What about recursive?

    # TODO: a (env var) switch to use tmp location rather than user's cache?
    #       would be: persistent across tests vs persistent across test runs.

    if not dataset_name:
        dataset_name = decode_source_spec(url)['default_destpath']

    ds = Dataset(DATALAD_TESTS_CACHE / dataset_name)
    if not ds.is_installed():
        ds = Clone()(url, ds.pathobj)
    # TODO: check version
    ds.get()
    return ds


@optional_args
def datalad_dataset(f, url=None, version=None, name=None):

    # TODO: env var to enable/disable caching? Or melt with location switch
    #       in get_cached_dataset?

    @better_wraps(f)
    @with_tempfile
    def newfunc(*arg, **kw):
        ds = get_cached_dataset(url, name, version=version)
        clone_ds = Clone()(ds.pathobj, arg[-1])
        print("\n\n Cloned from cache to: %s" % arg[-1])
        print("\n\n Deliver dataset: %s" % clone_ds)
        return f(*(arg[:-1] + (clone_ds,)), **kw)

    return newfunc
