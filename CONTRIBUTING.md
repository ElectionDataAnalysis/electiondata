Please note that electiondata is released with a [Contributor Code of Conduct](CODE_OF_CONDUCT.md). 
By contributing to this project, 
you agree to abide by its terms.

# Contributing to electiondata development

## Report a Problem

To report a problem, [file an issue on GitHub](https://github.com/ElectionDataAnalysis/electiondata/issues). When filing an issue, the most important thing is to include a minimal 
reproducible example so that we can quickly verify the problem, and then figure 
out how to fix it. Please include

1.  Link to the exact version of electiondata you are using.
  
1.  A copy of your working directory, following the model in the [Sample Session](docs/Sample_Session.md). Include:
    * a `run_time.ini` file (Feel free to redact the login information for your local postgres instance.) 
    * all subdirectories referenced in the `run_time.ini` file. 
    * If the issue involves loading a particular results data file, be sure to include that file. If possible, avoid submitting large results files -- if your file is precinct-based, for example, see if you can demonstrate the issue with a truncated results file.
    * all error and warnings files placed by the system into the `reports_and_plots_dir` specified in `run_time.ini`.
  
1.  A transcript of the python session, where python is called from the working directory.

You can check you have actually made a reproducible example by:
1. creating a virtual environment
1. installing the indicated version of electiondata
1. if files or folders were moved by the system to the archive directory, move them back to the input directory, removing any timestamps from directory names.
1. navigating to the working directory, calling python, and producing the behavior in question.

## Revise the Code

To contribute a change to `electiondata` follow these steps:

1. Create a branch in git and make your changes.
1. Run unit tests to check for broken functionality. See [pytest instructions](docs/Testing_Code_with_pytest.md).
1. Run python `black` to format your code to our standard.
1. Push branch to github and issue pull request (PR).
1. Discuss the pull request.
1. Iterate until either we accept the PR or decide that it's not
   a good fit for `electiondata`.

Each of these steps are described in more detail below. This might feel 
overwhelming the first time you get set up, but it gets easier with practice. 
If you get stuck at any point, feel free to [contact us](CONTACT_US.md) for help.

If you're not familiar with git or github, please read a tutorial such as [https://realpython.com/python-git-github-intro/](https://realpython.com/python-git-github-intro/).


Pull requests will be evaluated against this checklist:

1.  __Motivation__. Your pull request should clearly and concisely motivate the
    need for change.

1.  __Only related changes__. Before you submit your pull request, please
    check to make sure that you haven't accidentally included any unrelated
    changes. These make it harder to see exactly what's changed, and to
    evaluate any unexpected side effects.

    Each PR corresponds to a git branch, so if you expect to submit
    multiple changes make sure to create multiple branches. If you have
    multiple changes that depend on each other, start with the first one
    and don't submit any others until the first one has been processed.
    
1.  __Documentation__ Any new parameters or a new functions must be documented both in the code and in the [User Guide](docs/User_Guide.md), [Sample Session](docs/Sample_Session.md) and any other relevant documents. If you're adding a new graphical or analytical feature, please add a short example to [Sample Session](docs/Sample_Session.md).

1.  __Tests__ If fixing a bug or adding a new feature to a non-graphical function,
    please add a [pytest](https://docs.pytest.org) unit test. Document the new test in [pytest instructions](docs/Testing_Code_with_pytest.md).

This seems like a lot of work but don't worry if your pull request isn't perfect.
Unless you've submitted a few in the
past it's unlikely that your pull request will be accepted as is. All PRs require
review and approval from at least one member of the `electiondata` development team 
before merge.

