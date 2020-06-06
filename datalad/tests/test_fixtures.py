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
from datalad.utils import (
    opj,
    Path,
)
from datalad.tests.utils import (
    assert_equal,
    assert_false,
    assert_in,
    assert_is,
    assert_is_instance,
    assert_not_in,
    assert_not_equal,
    assert_raises,
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

        # tuples to test (url, name, version, paths, class):
        test_cases = [

            # a simple testrepo
            ("https://github.com/datalad/testrepo--minimalds",
             None,
             "541cf855d13c2a338ff2803d4488daf0035e568f",
             None,
             AnnexRepo),
            # Same repo, but request paths to be present. This should work
            # with a subsequent call, although the first one did not already
            # request any.
            ("https://github.com/datalad/testrepo--minimalds",
             None,
             "9dd8b56cc706ab56185f2ceb75fbe9de9b606724",
             opj('inannex', 'animated.gif'),
             AnnexRepo),
            # Same repo again, but invalid version
            ("https://github.com/datalad/testrepo--minimalds",
             None,
             "nonexistent",
             opj("irrelevant", "path"),  # invalid version -> path irrelevant
             AnnexRepo),
            # same thing with different name should be treated as a new thing
            ("https://github.com/datalad/testrepo--minimalds",
             "minimal",
             "git-annex",
             None,
             AnnexRepo),
            # try a plain git repo to make sure we can deal with that
            # TODO: version + paths
            ("https://github.com/datalad/datalad.org",
             None,
             None,
             None,
             GitRepo),

        ]
        for url, name, version, paths, cls in test_cases:
            target = cache_dir / (name if name
                                  else dec_url(url)['default_destpath'])

            # assuming it doesn't exist yet - patched cache dir!
            in_cache_before = target.exists()
            with patch("datalad.tests.fixtures.Clone.__call__") as exec_clone:
                try:
                    ds = get_cached_dataset(url, name, version, paths)
                    invalid_version = False
                except AssertionError:
                    # should happen only if `version` wasn't found. Implies
                    # that the dataset exists in cache (although not returned
                    # due to execpetion)
                    assert_true(version)
                    assert_false(Dataset(target).repo.commit_exists(version))
                    # mark for later assertions (most of them should still hold
                    # true)
                    invalid_version = True

            if not in_cache_before:
                # clone was called
                # Note: assert_called was only introduced in 3.6, while
                # assert_not_called exists in 3.5 already. As we test for python
                # 3.5, we need to solve it differently. At the same time the
                # test isn't about the precise call as required by
                # assert_called_with.
                # -> just negate assert_not_called
                assert_raises(AssertionError, exec_clone.assert_not_called)
            else:
                exec_clone.assert_not_called()

            # Patch prevents actual execution. Now do it for real. Note, that
            # this might be necessary for content retrieval even if dataset was
            # in cache before,
            try:
                ds = get_cached_dataset(url, name, version, paths)
            except AssertionError:
                # see previous call
                assert_true(invalid_version)

            assert_is_instance(ds, Dataset)
            assert_true(ds.is_installed())
            assert_equal(target, ds.pathobj)
            assert_is_instance(ds.repo, cls)

            # TODO: paths to get and `version` parameter are not aligned yet. We
            #       actually need to deal with keys instead to be able to refer
            #       to content not in current worktree!
            if paths and not invalid_version:
                # Note: it's not supposed to get that content if passed
                # `version` wasn't available. get_cached_dataset would then
                # raise before and not download anything only to raise
                # afterwards.
                has_content = ds.repo.file_has_content(paths)
                assert_true(
                    all(has_content) if isinstance(has_content, list)
                    else has_content
                )

            # version check. Note, that all `get_cached_dataset` is supposed to
            # do, is verifying, that specified version exists - NOT check it
            # out"
            if version and not invalid_version:
                assert_true(ds.repo.commit_exists(version))

            # re-execution
            with patch("datalad.tests.fixtures.Clone.__call__") as exec_clone:
                try:
                    ds2 = get_cached_dataset(url, name, version, paths)
                except AssertionError:
                    assert_true(invalid_version)
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

        @cached_dataset(url=ds_url,
                        version="541cf855d13c2a338ff2803d4488daf0035e568f")
        def decorated_test5(ds):
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

            assert_equal(ds.repo.get_hexsha(),
                         "541cf855d13c2a338ff2803d4488daf0035e568f")

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
