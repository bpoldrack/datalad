"""utilities to assist with setting up tests"""

from datalad import cfg
from datalad.core.distributed.clone import (
    Clone,
    decode_source_spec
)
from datalad.distribution.dataset import Dataset
from datalad.utils import (
    better_wraps,
    ensure_list,
    optional_args,
)
from datalad.tests.utils import with_tempfile


DATALAD_TESTS_CACHE = cfg.obtain("datalad.tests.cache")


def get_cached_dataset(url, dataset_name=None, version=None, paths=None):
    """ Helper to get a cached clone from url

    Intended for use from within `cached_dataset` and `cached_url` decorators.
    Clones `url` into user's cache under datalad/tests/`name`. If such a clone
    already exists, don't clone but return the existing one. So, it's supposed
    to cache the original source in order to reduce time and traffic for tests,
    by letting subsequent requests clone from a local location directly.

    If it's an annex get the content as provided by `paths`, too.

    Verifies that `version` can be checked out, but doesn't actually do it,
    since the cached dataset is intended to be used as origin instead of the
    original remote at URL by the `cached_dataset` test decorator. Checkout of
    a particular version should happen in its clone.

    Parameters
    ----------
    url: str
        URL to clone from
    dataset_name: str or None
        (directory) name to use for the clone. If None, a name will be derived
        from `url`.
    paths: str or list or None
        (list of) paths to get content for. Passed to datalad-get.
    version: str or None
        A commit or an object that can be dereferenced to one.

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
    if version:
        # check whether version is available
        assert ds.repo.commit_exists(version)
    # TODO: What about an outdated dataset in cache? How do we properly update
    #       from original URL in all cases? Given that it's meant to be used in
    #       tests, it seems okay-ish to just manually remove from cache if tests
    #       appear to not work right (or a version is missing). Datasets used in
    #       tests shouldn't change all the time.
    #       Auto-fetching, -pulling etc. seems difficult to cover all cases,
    #       particularly if `version` specifies local branches.
    if paths:
        # TODO: Double-check we can actually get keys rather than paths.
        ds.get(ensure_list(paths))

    return ds


@optional_args
def cached_dataset(f, url=None, name=None, version=None, paths=None):
    """Test decorator providing a clone of `url` from cache

    If config datalad.tests.cache is not set, delivers a clone in a temporary
    location of the original `url`. Otherwise that clone is in fact a clone of a
    cached dataset (origin being the cache instead of `url`).
    This allows to reduce time and network traffic when using a dataset in
    different tests.

    Parameters
    ----------
    f
    url
    name
    version
    paths

    Returns
    -------

    """
    @better_wraps(f)
    @with_tempfile
    def newfunc(*arg, **kw):
        if DATALAD_TESTS_CACHE:
            ds = get_cached_dataset(url, dataset_name=name,
                                    version=version, paths=paths)
            clone_ds = Clone()(ds.pathobj, arg[-1])
        else:
            clone_ds = Clone()(url, arg[-1])
        if version:
            clone_ds.repo.checkout(version)
        # TODO: with version implemented, we would actually need to not pass
        #       `paths` into get_cached_dataset, but figure the keys based on
        #       version checkout in the clone und get those keys in `ds`. Only
        #       then get paths in `clone_ds` from `ds`.
        if paths:
            clone_ds.get(ensure_list(paths))
        return f(*(arg[:-1] + (clone_ds,)), **kw)

    return newfunc


@optional_args
def cached_url(f, url=None, name=None, paths=None):
    """Test decorator providing a URL to clone from, pointing to cached dataset

    If config datalad.tests.cache is not set, delivers the original `url`,
    otherwise a file-scheme url to the cached clone thereof.

    Parameters
    ----------
    f
    url
    name
    paths

    Returns
    -------

    """
    @better_wraps(f)
    def newfunc(*arg, **kw):
        if DATALAD_TESTS_CACHE:
            ds = get_cached_dataset(url, dataset_name=name,
                                    version=None, paths=paths)
            new_url = ds.pathobj.as_uri()
        else:
            new_url = url

        return f(*(arg + (new_url,)), **kw)

    return newfunc
