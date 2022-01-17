# How to contribute Apache Arrow Julia

## Did you find a bug or have an improvement?

We recommend you first search among existing [Github issues](https://github.com/apache/arrow-julia/issues). The community may already address the same idea. If you could find the issue, you may want to contribute to the existing issue.


## How do you write a patch that fixes a bug or brings an improvement? 
If you cannot find the same idea in the issues, you first need to write a GitHub issue (e.g. [issues in Arrow-julia](https://github.com/apache/arrow-julia/issues)) for a bug fix or planned features for the improvement. To write an issue would help the community have visibility and opportunities for collaborations before a pull request (PR) shows up. This is for the [Apache way](http://theapacheway.com/). We can use GitHub labels to identify bugs.   
It should not be necessary to file an issue for some non-code changes, such as CI changes or minor documentation updates such as fixing typos.

After writing the issue, you may want to write a code by creating [a PR](https://github.com/apache/arrow-julia/pulls). In the PR, it is preferable to refer to the issue number (e.g. `#1`) that you already created.


## Do you want to propose a significant new feature or an important refactoring?

We ask that all discussions about major changes in the codebase happen publicly on the [arrow-dev mailing-list](https://lists.apache.org/list.html?dev@arrow.apache.org).


## Do you have questions about the source code, the build procedure or the development process?

You can also ask on the mailing-list, see above.


## Local Development

When developing on Arrow.jl it is recommended that you run the following to ensure that any changes to ArrowTypes.jl are immediately available to Arrow.jl without requiring a release:

```
julia --project -e 'using Pkg; Pkg.develop(path="src/ArrowTypes")'
```


## Release cycle

The Julia community would like an independent release cycle. Release for apache/arrow doesn't include the Julia implementation. The Julia implementation uses separated version scheme. (apache/arrow uses 6.0.0 as the next version but the next Julia implementation release doesn't use 6.0.0.)

