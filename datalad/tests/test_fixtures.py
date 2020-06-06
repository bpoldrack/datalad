"""Testing test fixtures"""

from datalad.core.distributed.clone import decode_source_spec as dec_url
from datalad.distribution.dataset import Dataset
from datalad.support.annexrepo import AnnexRepo
from datalad.support.gitrepo import GitRepo
from datalad.tests.fixtures import (
    get_cached_dataset,
    cached_dataset,
    cached_url
)
from datalad.utils import Path
from datalad.tests.utils import (
    assert_equal,
    assert_false,
    assert_in,
    assert_is,
    assert_is_instance,
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
def test_cached_dataset(cache_dir):

    # patch DATALAD_TESTS_CACHE to not use the actual cache with
    # the test testing that very cache.
    cache_dir = Path(cache_dir)

    ds_url = "https://github.com/datalad/testrepo--minimalds"
    annexed_file = Path("inannex") / "animated.gif"

    with patch("datalad.tests.fixtures.DATALAD_TESTS_CACHE", new=cache_dir):

        @cached_dataset(url=ds_url)
        def decorated_test1(ds):
            # we get a Dataset instance
            assert_is_instance(ds, Dataset)
            # it's a clone in a temp. location, not within the cache
            assert_not_in(cache_dir, ds.pathobj.parents)
            assert_result_count(ds.siblings(), 1, type="sibling",
                                name="origin",
                                url=str(cache_dir / "testrepo--minimalds"))
            here = ds.config.get("annex.uuid")
            origin = ds.config.get("remote.origin.annex-uuid")
            where = ds.repo.whereis(str(annexed_file))
            assert_not_in(here, where)
            assert_not_in(origin, where)

            return ds.pathobj, ds.repo.pathobj

        @cached_dataset(url=ds_url, paths=str(annexed_file))
        def decorated_test2(ds):
            # we get a Dataset instance
            assert_is_instance(ds, Dataset)
            # it's a clone in a temp. location, not within the cache
            assert_not_in(cache_dir, ds.pathobj.parents)
            assert_result_count(ds.siblings(), 1, type="sibling",
                                name="origin",
                                url=str(cache_dir / "testrepo--minimalds"))
            here = ds.config.get("annex.uuid")
            origin = ds.config.get("remote.origin.annex-uuid")
            where = ds.repo.whereis(str(annexed_file))
            assert_in(here, where)
            assert_in(origin, where)

            return ds.pathobj, ds.repo.pathobj

        @cached_dataset(url=ds_url)
        def decorated_test3(ds):
            # we get a Dataset instance
            assert_is_instance(ds, Dataset)
            # it's a clone in a temp. location, not within the cache
            assert_not_in(cache_dir, ds.pathobj.parents)
            assert_result_count(ds.siblings(), 1, type="sibling",
                                name="origin",
                                url=str(cache_dir / "testrepo--minimalds"))
            # origin is the same cached dataset, that got this content in
            # decorated_test2 before. Should still be there. But "here" we
            # didn't request it
            here = ds.config.get("annex.uuid")
            origin = ds.config.get("remote.origin.annex-uuid")
            where = ds.repo.whereis(str(annexed_file))
            assert_not_in(here, where)
            assert_in(origin, where)

            return ds.pathobj, ds.repo.pathobj

        @cached_dataset(url=ds_url, name="different")
        def decorated_test4(ds):
            # we get a Dataset instance
            assert_is_instance(ds, Dataset)
            # it's a clone in a temp. location, not within the cache
            assert_not_in(cache_dir, ds.pathobj.parents)
            assert_result_count(ds.siblings(), 1, type="sibling",
                                name="origin",
                                url=str(cache_dir / "different"))
            here = ds.config.get("annex.uuid")
            origin = ds.config.get("remote.origin.annex-uuid")
            where = ds.repo.whereis(str(annexed_file))
            assert_not_in(here, where)
            assert_not_in(origin, where)

            return ds.pathobj, ds.repo.pathobj

        first_dspath, first_repopath = decorated_test1()
        second_dspath, second_repopath = decorated_test2()
        decorated_test3()
        decorated_test4()

        # first and second are not the same, only their origin is:
        assert_not_equal(first_dspath, second_dspath)
        assert_not_equal(first_repopath, second_repopath)


@with_tempfile(mkdir=True)
def test_cached_url(cache_dir):

    # patch DATALAD_TESTS_CACHE to not use the actual cache with
    # the test testing that very cache.
    cache_dir = Path(cache_dir)

    ds_url = "https://github.com/datalad/testrepo--minimalds"
    annexed_file = Path("inannex") / "animated.gif"

    with patch("datalad.tests.fixtures.DATALAD_TESTS_CACHE", new=cache_dir):

        @cached_url(url=ds_url)
        def decorated_test1(url):
            # we expect a file-scheme url to a cached version of `ds_url`
            expect_origin_path = cache_dir / "testrepo--minimalds"
            assert_equal(expect_origin_path.as_uri(),
                         url)
            origin = Dataset(expect_origin_path)
            assert_true(origin.is_installed())
            assert_false(origin.repo.file_has_content(str(annexed_file)))

        decorated_test1()

        @cached_url(url=ds_url, name="different", paths=str(annexed_file))
        def decorated_test2(url):
            # we expect a file-scheme url to a "different" cached version of
            # `ds_url`
            expect_origin_path = cache_dir / "different"
            assert_equal(expect_origin_path.as_uri(),
                         url)
            origin = Dataset(expect_origin_path)
            assert_true(origin.is_installed())
            assert_true(origin.repo.file_has_content(str(annexed_file)))

        decorated_test2()

    # disable caching. Note, that in reality DATALAD_TESTS_CACHE is determined
    # on import time of datalad.tests.fixtures based on the config
    # "datalad.tests.cache". We patch the result here, not the config itself.
    with patch("datalad.tests.fixtures.DATALAD_TESTS_CACHE", new=None):

        @cached_url(url=ds_url)
        def decorated_test3(url):
            # we expect the original url, since caching is disabled
            assert_equal(url, ds_url)

        decorated_test3()
