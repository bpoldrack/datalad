"""utilities to assist with setting up tests"""

from datalad import cfg
from datalad.core.distributed.clone import (
    Clone,
    decode_source_spec
)
from datalad.distribution.dataset import Dataset
from datalad.interface.common_cfg import dirs
from datalad.utils import (
    better_wraps,
    ensure_list,
    optional_args,
    Path
)
from datalad.tests.utils import with_tempfile


DATALAD_TESTS_CACHE = cfg.obtain("datalad.tests.cache")


def get_cached_dataset(url, dataset_name=None, version=None, paths=None):
    """ Helper to get a cached clone from url

    Intended for use from within `datalad_dataset` decorator.
    Clones `url` into user's cache under datalad/tests/`name`. If such a clone
    already exists, don't clone but return the existing one. So, it's supposed
    to cache the original source in order to reduce time and traffic for tests,
    by letting subsequent requests clone from a local location directly.

    If it's an annex get the content as provided by `paths`, too.

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
    paths: str or list
        (list of) paths to get content for. Passed to datalad-get

    Returns
    -------
    Dataset
    """

    # TODO: What about recursive? Might be complicated. We would need to make
    #       sure we can recursively clone _from_ here then.

    if not DATALAD_TESTS_CACHE:
        raise ValueError("Caching disabled by config")

    if not dataset_name:
        dataset_name = decode_source_spec(url)['default_destpath']

    ds = Dataset(DATALAD_TESTS_CACHE / dataset_name)
    if not ds.is_installed():
        ds = Clone()(url, ds.pathobj)
    # TODO: check version
    if paths:
        ds.get(ensure_list(paths))
    return ds


@optional_args
def datalad_dataset(f, url=None, name=None, version=None, paths=None):

    @better_wraps(f)
    @with_tempfile
    def newfunc(*arg, **kw):
        if DATALAD_TESTS_CACHE:
            ds = get_cached_dataset(url, dataset_name=name,
                                    version=version, paths=paths)
            clone_ds = Clone()(ds.pathobj, arg[-1])
        else:
            clone_ds = Clone()(url, arg[-1])
        # TODO: with version implemented, we would actually need to not pass
        #       `paths` into get_cached_dataset, but figure the keys based on
        #       version checkout in the clone und get those keys in `ds`. Only
        #       then get paths in `clone_ds` from `ds`.
        if paths:
            clone_ds.get(ensure_list(paths))
        return f(*(arg[:-1] + (clone_ds,)), **kw)

    return newfunc
