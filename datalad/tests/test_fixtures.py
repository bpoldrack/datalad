"""Testing test fixtures"""

import appdirs
from datalad.core.distributed.clone import decode_source_spec as dec_url
from datalad.distribution.dataset import Dataset
from datalad.support.annexrepo import AnnexRepo
from datalad.support.gitrepo import GitRepo
from datalad.tests.fixtures import (
    get_cached_dataset,
    datalad_dataset
)
from datalad.utils import Path
from datalad.tests.utils import (
    assert_equal,
    assert_false,
    assert_is,
    assert_is_instance,
    assert_is_not,
    assert_not_in,
    assert_not_equal,
    assert_result_count,
    assert_true,
    with_tempfile
)
from unittest.mock import patch


@with_tempfile(mkdir=True)
def test_get_cached_dataset(cache_dir):

    # patch DATALAD_TESTS_CACHE to not use the actual cache with
    # the test testing that very cache.
    cache_dir = Path(cache_dir)

    with patch("datalad.tests.fixtures.DATALAD_TESTS_CACHE", new=cache_dir):

        test_cases = [
            # url, name, version, class
            # a simple testrepo
            ("https://github.com/datalad/testrepo--minimalds",
             None, None, AnnexRepo),
            # same thing with different name should be treated as a new thing
            ("https://github.com/datalad/testrepo--minimalds",
             "minimal", None, AnnexRepo),
            # try a plain git repo
            ("https://github.com/datalad/datalad.org",
             None, None, GitRepo),

        ]
        for url, name, version, cls in test_cases:
            target = cache_dir / (name if name
                                  else dec_url(url)['default_destpath'])

            # assuming it doesn't exist yet - patched cache dir!
            assert_false(target.exists())
            with patch("datalad.tests.fixtures.Clone.__call__") as exec_clone:
                ds = get_cached_dataset(url, name, version)
            # clone was called
            exec_clone.assert_called()

            # patch prevents actual execution. Now do it for real:
            ds = get_cached_dataset(url, name, version)
            assert_is_instance(ds, Dataset)
            assert_true(ds.is_installed())
            assert_equal(target, ds.pathobj)
            assert_is_instance(ds.repo, cls)

            # TODO: check content is present
            # TODO: version checks

            # re-execution
            with patch("datalad.tests.fixtures.Clone.__call__") as exec_clone:
                ds2 = get_cached_dataset(url, name, version)
            exec_clone.assert_not_called()
            # returns the same Dataset as before:
            assert_is(ds, ds2)


@with_tempfile(mkdir=True)
def test_datalad_dataset(cache_dir):

    # patch DATALAD_TESTS_CACHE to not use the actual cache with
    # the test testing that very cache.
    cache_dir = Path(cache_dir)

    with patch("datalad.tests.fixtures.DATALAD_TESTS_CACHE", new=cache_dir):

        @datalad_dataset(url="https://github.com/datalad/testrepo--minimalds")
        def decorated_test1(ds):
            # we get a Dataset instance
            assert_is_instance(ds, Dataset)
            # it's a clone in a temp. location, not within the cache
            assert_not_in(cache_dir, ds.pathobj.parents)
            assert_result_count(ds.siblings(), 1, type="sibling",
                                name="origin",
                                url=str(cache_dir / "testrepo--minimalds"))
            return ds.pathobj, ds.repo.pathobj

        @datalad_dataset(url="https://github.com/datalad/testrepo--minimalds")
        def decorated_test2(ds):
            # we get a Dataset instance
            assert_is_instance(ds, Dataset)
            # it's a clone in a temp. location, not within the cache
            assert_not_in(cache_dir, ds.pathobj.parents)
            assert_result_count(ds.siblings(), 1, type="sibling",
                                name="origin",
                                url=str(cache_dir / "testrepo--minimalds"))

            return ds.pathobj, ds.repo.pathobj

        @datalad_dataset(url="https://github.com/datalad/testrepo--minimalds",
                         name="different")
        def decorated_test3(ds):
            # we get a Dataset instance
            assert_is_instance(ds, Dataset)
            # it's a clone in a temp. location, not within the cache
            assert_not_in(cache_dir, ds.pathobj.parents)
            assert_result_count(ds.siblings(), 1, type="sibling",
                                name="origin",
                                url=str(cache_dir / "different"))

            return ds.pathobj, ds.repo.pathobj

        first_dspath, first_repopath = decorated_test1()
        second_dspath, second_repopath = decorated_test2()
        decorated_test3()

        # first and second are not the same, only their origin is:
        assert_not_equal(first_dspath, second_dspath)
        assert_not_equal(first_repopath, second_repopath)
