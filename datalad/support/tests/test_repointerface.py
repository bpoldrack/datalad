# emacs: -*- mode: python; py-indent-offset: 4; tab-width: 4; indent-tabs-mode: nil -*-
# ex: set sts=4 ts=4 sw=4 noet:
# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##
#
#   See COPYING file distributed along with the datalad package for the
#   copyright and license terms.
#
# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##
"""Test implementation of class RepoInterface

"""


from datalad.tests.utils import *


@with_tempfile
@with_tempfile
def test_has_annex(here, there):
    from datalad.support.gitrepo import GitRepo
    from datalad.support.annexrepo import AnnexRepo
    gr = GitRepo(path=here, create=True)
    assert_false(gr.has_annex())
    ar = AnnexRepo(path=here, create=True)
    assert_true(ar.has_annex())
    gr2 = GitRepo.clone(path=there, url=here, create=True)
    assert_true(gr2.has_annex())
