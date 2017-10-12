# emacs: -*- mode: python; py-indent-offset: 4; tab-width: 4; indent-tabs-mode: nil -*-
# ex: set sts=4 ts=4 sw=4 noet:
# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##
#
#   See COPYING file distributed along with the datalad package for the
#   copyright and license terms.
#
# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##

"""datalad's test repository mechanism
"""

import os

from abc import ABCMeta, abstractmethod
from os.path import join as opj

from six import add_metaclass

from datalad.cmd import GitRunner
from datalad.support.network import get_local_file_url

from datalad.tests.testrepos.exc import InvalidTestRepoDefinitionError, \
    TestRepoCreationError
from datalad.tests.testrepos.items import Item, ItemRepo, ItemSelf, ItemFile, \
    ItemInfoFile, ItemCommand, ItemCommit, ItemDropFile
from datalad.tests.utils import eq_, assert_is_instance, assert_in

from datalad.utils import assure_list
from datalad.utils import auto_repr

from .utils import remote_file_path

# new TestRepo* classes:
# - always a "tmp" location or configurable via "datalad.tests.<something>"?
# - assert_unchanged() method
# - properties to
# - (annex) init optional? don't think so (well, might be useful for testing). But: Should be possible to have an
#   uninitialized submodule and a corresponding property







@auto_repr
@add_metaclass(ABCMeta)
class TestRepo_NEW(object):  # object <=> ItemRepo?
    """Base class for test repositories
    """

    # version of that test repository; might be used to determine a needed
    # update if used persistently
    version = '0.1'  # TODO: version for abstract class? May be none at all

    # properties accessible by tests in order to base their assertions on it:

    # keys(annex)
    # commit SHA(s)?
    # branches

    # the following might be combined in a single data structure:
    # - files/files in git/files in annex
    # - submodules/hierarchy? dict with files/repos and their properties?
    # - status (what's untracked/staged/locked/unlocked/etc.')

    # Should there be remote location available to install/clone from?
    # => needs to be optional, since cloning would loose untracked/staged things
    #    as well as other branches. So it's possibly not reasonable for some of
    #    the test repos

    # list of commands to execute in order to create this test repository
    _item_definitions = []
    # list of tuples: Item's class and kwargs for constructor
    # Note:
    # - item references are paths
    # - 'path' and 'cwd' arguments are relative to TestRepo's root
    # - toplevel repo: path = None ???

    # example:
    # _item_list = [(ItemRepo, {'path': 'somewhere', 'annex': False, ...})
    #           (ItemFile, {'path': os.path.join('somewhere', 'beneath'),
    #                       'content': 'some content for the file',
    #                       'untracked': True})
    #          ]

    def __init__(self, path=None, runner=None):

        # TODO: Probably the same mechanism has to be applied for
        # ItemFile(src=...) in order to be able to assign predefined
        # ItemFile instances!
        def _path2item(def_item, def_idx, kw):
            """internal helper to convert item references in definitions

            Items need to be referenced by path in TestRepo definitions.
            Converts those paths and assigns the actual objects instead.

            Parameters
            ----------
            def_item: tuple
                an entry of _item_definition
            def_idx: int
                index of that entry
            kw: str
                the keyword to convert
            """

            ref_it = def_item[1].get(kw)
            if ref_it:
                # might be a list, therefore always treat as such
                ref_it = assure_list(ref_it)
                def_item[1][kw] = []
                for r in ref_it:
                    try:
                        def_item[1][kw].append(self._items[r])
                    except KeyError:
                        raise InvalidTestRepoDefinitionError(
                            "Item {it} referenced before definition:"
                            "{ls}{cl}({args})"
                            "".format(ls=os.linesep,
                                      it=r,
                                      cl=def_item[0].__name__,
                                      args=def_item[1]),
                            repo=self.__class__,
                            item=def_item[0].__name__,
                            index=def_idx
                        )

                # if it's not a list undo assure_list:
                if len(def_item[1][kw]) == 1:
                    def_item[1][kw] = def_item[1][kw][0]

        self._path = path
        # TODO
        # check path!  => look up, how it is done now in case of persistent ones
        # Note: If we want to test whether an existing one is valid, we need to
        # do it after instantiation of items.
        # But: Probably just fail. Persistent ones are to be kept in some kind
        # of registry and delivered by with_testrepos without trying to
        # instantiate again.

        self._runner = runner or GitRunner(cwd=path)
        self.repo = None
        self._items = {}
        self._execution = []

        for item, index in zip(self._item_definitions,
                               range(len(self._item_definitions))):

            if not (issubclass(item[0], Item) and isinstance(item[1], dict)):
                raise InvalidTestRepoDefinitionError(
                    msg="Malformed definition entry. An entry of a TestRepo's "
                        "definition list is expected to be a tuple, consisting "
                        "of a subclass of Item and a dict, containing kwargs "
                        "for its instantiation. Entry at index {idx} is "
                        "violating this constraint:{ls}{cl}({args})"
                        "".format(ls=os.linesep,
                                  cl=item[0],
                                  args=item[1]
                                  ),
                    repo=self.__class__,
                    item=item[0].__name__,
                    index=index
                )

            # 1. necessary adaptions of arguments for instantiation
            # pass the Runner if there's None:
            if item[1].get('runner', None) is None:
                item[1]['runner'] = self._runner

            if issubclass(item[0], ItemCommand):
                # commands need a 'cwd' or a 'repo' to run in.
                r_cwd = item[1].get('cwd')
                it_repo = item[1].get('repo')
                if not r_cwd and not it_repo:
                    raise InvalidTestRepoDefinitionError(
                        msg="Neither 'cwd' nor 'repo' was specified for {cl}. "
                            "At least one of those is required by ItemCommand:"
                            "{ls}{cl}({args})".format(ls=os.linesep,
                                                      cl=item[0].__name__,
                                                      args=item[1]),
                        repo=self.__class__,
                        item=item[0].__name__,
                        index=index
                    )

                # If 'repo' wasn't specified, we can try whether we already know
                # an ItemRepo at 'cwd' and pass it into 'repo'.
                # Note, that 'repo' isn't necessarily required by a command, but
                # most of them will need it and if not so, they just shouldn't
                # care, so we can safely pass one.
                if not it_repo:
                    repo_by_cwd = self._items.get(r_cwd)
                    if repo_by_cwd and isinstance(repo_by_cwd, ItemRepo):
                        # adjust definition, meaning we need to assign the
                        # relative path as if it was done by the user
                        item[1]['repo'] = r_cwd
                        it_repo = r_cwd

                # paths are relative in TestRepo definitions, but absolute in
                # the Item instances. Replace 'cwd' if needed.
                if r_cwd:
                    # store absolute path for instantiation
                    item[1]['cwd'] = os.path.normpath(opj(self._path, r_cwd))

                if it_repo:
                    # convert the reference in argument 'repo':
                    _path2item(item, index, 'repo')

                # convert item references in argument 'item':
                _path2item(item, index, 'item')

            if issubclass(item[0], ItemRepo) or issubclass(item[0], ItemFile):
                # paths are relative in TestRepo definitions, but absolute in
                # the Item instances. Replace them for instantiation, but keep
                # `r_path` for identification (key in the items dict).
                # Additionally 'path' is mandatory for ItemRepo and ItemFile.
                # Exception: ItemInfoFile has a default path
                r_path = item[1].get('path', None)
                if not r_path:
                    if issubclass(item[0], ItemInfoFile):
                        r_path = ItemInfoFile.default_path
                    else:
                        raise InvalidTestRepoDefinitionError(
                            msg="Missing argument 'path' for {cl}:{ls}"
                                "{cl}({args})".format(ls=os.linesep,
                                                      cl=item[0].__name__,
                                                      args=item[1]),
                            repo=self.__class__,
                            item=item[0].__name__,
                            index=index
                            )
                # `r_path` identifies an item; it must be unique:
                if r_path in self._items:
                    raise InvalidTestRepoDefinitionError(
                        msg="Ambiguous definition. 'path' argument for ItemRepo"
                            " and ItemFile instances must be unique. "
                            "Encountered second use of {p}:{ls}{cl}({args})"
                            "".format(ls=os.linesep,
                                      p=r_path,
                                      cl=item[0].__name__,
                                      args=item[1]),
                            repo=self.__class__,
                            item=item[0].__name__,
                            index=index
                        )

                # store absolute path for instantiation
                item[1]['path'] = os.path.normpath(opj(self._path, r_path))

            # check and convert 'repo' argument for files
            # TODO: We already have that (kind of) for ItemCommand. May be melt
            # in. Definitely do it, if we are to provide this also for ItemRepo
            # as an option to instantly submodule-add
            if issubclass(item[0], ItemFile):
                r_repo = item[1].get('repo')
                if not r_repo:
                    raise InvalidTestRepoDefinitionError(
                        msg="Missing argument 'repo' for {cl}:{ls}"
                            "{cl}({args})".format(s=os.linesep,
                                                  cl=item[0].__name__,
                                                  args=item[1]),
                            repo=self.__class__,
                            item=item[0].__name__,
                            index=index
                            )
                # convert to ItemRepo:
                _path2item(item, index, 'repo')

            # END path conversion
            # For ItemRepo and ItemFile the relative path is kept in `r_path`

            # special case ItemInfoFile
            if issubclass(item[0], ItemInfoFile):
                # pass TestRepo subclass to the info file:
                item[1]['class_'] = self.__class__
                # pass item definitions to the info file:
                item[1]['definition'] = self._item_definitions

            # 2. instantiate items
            # Note, that there are two stores of instances: self._items and
            # self._execution. ItemCommands are used for creation only and are
            # stored in self._execution only (which then is to be used by
            # self.create()).
            # Other items, namely ItemRepo and ItemFile objects are additionally
            # stored in self._items for later access by properties of
            # TestRepo_NEW and its subclasses.

            try:
                item_instance = item[0](**item[1])
            except InvalidTestRepoDefinitionError as e:
                # add information the Item classes can't know:
                e.repo = self.__class__
                e.index = index
                raise e

            self._execution.append(item_instance)
            if not issubclass(item[0], ItemCommand):
                self._items[r_path] = item_instance

            # 3. special case: save reference to "self":
            if item[0] is ItemSelf:
                if self.repo:
                    # we had one already
                    raise InvalidTestRepoDefinitionError(
                            "{cl} must not be defined multiple times. "
                            "Found a second definition:{ls}{cl}({args})"
                            "".format(ls=os.linesep,
                                      cl=ItemSelf,
                                      args=item[1]),
                            repo=self.__class__,
                            item=ItemSelf,
                            index=index
                    )
                self.repo = self._items[r_path]

        if not self.repo:
            raise InvalidTestRepoDefinitionError(
                msg="Definition must contain exactly one {cl}. Found none."
                    "".format(cl=ItemSelf),
                repo=self.__class__
            )

        # Now, actually create the beast physically:
        self.create()

        # There might be ItemRepo(s) besides the top-level one, that were never
        # added as a submodule to another one. For recursive calls of
        # assert_intact, we need all roots of this forrest. Furthermore, even
        # ItemSelf might not be a root.
        # We can discover them only now after all Items were created, since
        # ItemCommands may have changed what could have been discovered during
        # instantiation.
        self._roots = {self._items[p] for p in self._items
                       if isinstance(self._items[p], ItemRepo) and
                       self._items[p].superproject is None}

        # Note, that by now there's no limitation on whether or not there needs
        # to be an item '.'. Theoretically everything should work with several
        # hierarchies in parallel, self.path being there common root location.
        # However, if there is an item '.', it must not be an ItemFile. If so,
        # something went wrong.
        if self._items.get('.') and isinstance(self._items['.'], ItemFile):
            raise InvalidTestRepoDefinitionError(
                msg="Item at root location {p} must not be a file: {cl}('.')"
                    "".format(cl=self._items['.'].__class__,
                              p=self.path),
                item=self._items['.']
            )

    # properties pointing to ItemSelf (self.repo)!
    @property
    def path(self):
        return self.repo.path

    @property
    def url(self):
        return self.repo.url

    def assert_intact(self):
        """Assertions to run to check integrity of this test repository

        Should probably be enhanced by subclasses and is supposed to recursively
        call assert_intact of its items. Therefore, call it via super if you
        derive a new class!
        """

        # object consistency:

        assert_is_instance(self.repo, ItemSelf)
        [assert_is_instance(it, ItemRepo) for it in self._roots]
        [assert_is_instance(self._items[p], Item) for p in self._items]

        # everything, that's in the definition, needs to be in the execution
        # list
        # TODO: come up with a better idea than just testing length. Note, that
        # execution can't be a dict and definition does not contain actual
        # instances
        eq_(len(self._item_definitions), len(self._execution))
        eq_(set(self._items[p] for p in self._items),
            set(it for it in self._execution if not isinstance(it, ItemCommand)))

        # all items are recursively accessible via self._roots:
        def get_items_recursively(item):
            # everything directly underneath
            items = item._items
            result = set(items)
            # plus recursively all subrepos
            print "initial set: %s" % result
            for it in items:
                if isinstance(it, ItemRepo):
                    result = result.union(get_items_recursively(it))
                    print "current set: %s" % result

            return result

        reachable = self._roots
        for it in self._roots:
            reachable = reachable.union(get_items_recursively(it))
        eq_(reachable, set(self._items[p] for p in self._items))

        # check them recursively:
        [item.assert_intact() for item in self._roots]

        # TODO: Is there more we can test by default?

    def create(self):
        """Physically create the beast
        """
        # default implementation:
        for item, index in zip(self._execution, range(len(self._execution))):
            try:
                item.create()
            except TestRepoCreationError as e:
                # add information the Item classes can't know:
                e.repo = self.__class__
                e.index = index
                raise e




#
#  Actual test repositories:
#

@auto_repr
class BasicGit(TestRepo_NEW):
    """Simple plain git repository

    RF'ing note: This resembles the old `BasicGitTestRepo`. The only difference
    is the content of INFO.txt, which is now more detailed. In particular it
    includes the entire definition of this test repository.
    """

    version = '0.1'

    _item_definitions = [(ItemSelf, {'path': '.',
                                     'annex': False}),
                         (ItemInfoFile, {'state': (ItemFile.ADDED,
                                                   ItemFile.UNMODIFIED),
                                         'repo': '.'}),
                         (ItemFile, {'path': 'test.dat',
                                     'content': "123",
                                     'annexed': False,
                                     'state': (ItemFile.ADDED,
                                               ItemFile.UNMODIFIED),
                                     'repo': '.'}),

                         (ItemCommit, {'cwd': '.',
                                       'item': ['test.dat', 'INFO.txt'],
                                       'msg': "Adding a basic INFO file and "
                                              "rudimentary load file."})
                         ]

    def __init__(self, path=None, runner=None):
        super(BasicGit, self).__init__(path=path, runner=runner)

    def assert_intact(self):

        super(BasicGit, self).assert_intact()

        # ###
        # Assertions to test object properties against what is defined:
        # ###
        eq_(len(self._items), 3)  # ItemRepo and ItemFile only
        assert_is_instance(self._items['.'], ItemSelf)
        assert_is_instance(self._items['test.dat'], ItemFile)
        assert_is_instance(self._items['INFO.txt'], ItemFile)

        # the top-level item `self.repo`
        assert(self.repo is self._items['.'])
        assert(self.repo.is_annex is False)
        assert(self.repo.is_git is True)

        for att in ['annex_version',
                    'is_direct_mode',
                    'annex_is_initialized',
                    'remotes',
                    'submodules',
                    'superproject']:
            value = self.repo.__getattribute__(att)
            assert(value is None,
                   "ItemSelf({p}).{att} is not None but: {v}"
                   "".format(p=self.repo.path, att=att, v=value))

        eq_([c[1] for c in self.repo.commits],
            ["Adding a basic INFO file and rudimentary load file."])
        # TODO: eq_(self.repo.branches, ['master'])

        # test.dat:
        test_dat = self._items['test.dat']
        assert_is_instance(test_dat, ItemFile)
        eq_(test_dat.path, opj(self.path, 'test.dat'))
        eq_(test_dat.content, "123")

        # INFO.txt:
        info_txt = self._items['INFO.txt']
        eq_(info_txt.path, opj(self.path, 'INFO.txt'))
        # Note: we can't compare the entire content of INFO.txt, since
        # it contains versions of git, git-annex, datalad. But some parts can
        # be expected and shouldn't change, so make assertions to indicate
        # integrity of the file's content:
        assert_is_instance(info_txt, ItemInfoFile)

        # both objects make up `files` of ItemSelf:
        eq_(set(self.repo.files), {test_dat, info_txt})

        # for both files the following should be true:
        for file_ in [test_dat, info_txt]:
            file_.assert_intact()
            assert(file_.is_clean is True)
            for att in ['annexed', 'is_modified', 'is_staged', 'is_untracked']:
                value = file_.__getattribute__(att)
                assert(value is False,
                       "ItemFile({p}).{att} is not False but: {v}"
                       "".format(p=file_.path, att=att, v=value))
            for att in ['annex_key', 'content_available', 'is_unlocked']:
                value = file_.__getattribute__(att)
                assert(value is None,
                       "ItemFile({p}).{att} is not None but: {v}"
                       "".format(p=file_.path, att=att, v=value))
            eq_(len(file_.commits), 1)
            eq_([c[1] for c in file_.commits],
                ["Adding a basic INFO file and rudimentary load file."])

        # ###
        # The objects' inner consistency and testing against what is physically
        # the case is done recursively via their respective assert_intact().
        # Note, that this requires to call assert_intact of all ItemRepo
        # instances in this TestRepo, that have no superproject.
        # In most cases this will just be one call to the toplevel instance:
        # ###
        # TODO: The note above might change a little, once it is clear what to
        # do about unregistered subs and untracked files.
        # TODO: May be that part can be done by TestRepo anyway, if it gets more
        # complicated. Then assert_intact wouldn't be abstract, but to be
        # enhanced.
        self.repo.assert_intact()


@auto_repr
class BasicMixed(TestRepo_NEW):
    """Simple mixed repository

    RF'ing note: This resembles the old `BasicAnnexTestRepo`. The only difference
    is the content of INFO.txt, which is now more detailed. In particular it
    includes the entire definition of this test repository.
    The renaming takes into account, that this repository has a file in git,
    which turns out to be not that "basic" as an annex with no file in git due
    to issues with annex repository version 6.
    """

    version = '0.1'

    _item_definitions = [(ItemSelf, {'path': '.',
                                     'annex': True}),
                         (ItemInfoFile, {'state': (ItemFile.ADDED,
                                                   ItemFile.UNMODIFIED),
                                         'repo': '.'}),
                         (ItemFile, {'path': 'test.dat',
                                     'content': "123",
                                     'annexed': False,
                                     'state': (ItemFile.ADDED,
                                               ItemFile.UNMODIFIED),
                                     'repo': '.'}),

                         (ItemCommit, {'cwd': '.',
                                       'item': ['test.dat', 'INFO.txt'],
                                       'msg': "Adding a basic INFO file and "
                                              "rudimentary load file for annex "
                                              "testing"}),
                         (ItemFile, {'path': 'test-annex.dat',
                                     'src': get_local_file_url(remote_file_path),
                                     'state': (ItemFile.ADDED,
                                               ItemFile.UNMODIFIED),
                                     'annexed': True,
                                     'key': "SHA256E-s28--2795fb26981c5a687b9bf44930cc220029223f472cea0f0b17274f4473181e7b.dat",
                                     'repo': '.'
                                     }),
                         (ItemCommit, {'cwd': '.',
                                       'item': 'test-annex.dat',
                                       'msg': "Adding a rudimentary git-annex load file"}),
                         (ItemDropFile, {'cwd': '.',
                                         'item': 'test-annex.dat'})
                         ]

    def __init__(self, path=None, runner=None):
        super(BasicMixed, self).__init__(path=path, runner=runner)

    def assert_intact(self):
        # fake sth:
        assert "everything is fine"


class BasicAnnex(TestRepo_NEW):
    pass


# 4 times: untracked, modified, staged, all of them
class BasicGitDirty(BasicGit):
    pass




# see above (staged: annex, git, both)
class BasicAnnexDirty(BasicAnnex):
    pass


# ....


# v6 adjusted branch ...

# Datasets (.datalad/config, .datalad/metadata ...) ?