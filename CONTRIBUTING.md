# Contributing to R2DBC MySQL

R2DBC MySQL is released under the Apache 2.0 license. If you would like to contribute something, or want to
hack on the code this document should help you get started.

## Code of Conduct

This project adheres to the [Contributor Covenant Code of Conduct](CODE_OF_CONDUCT.md). By participating, you
are expected to uphold this code.

## Using GitHub Issues

We use GitHub issues to track bugs and enhancements. If you have a general usage question please ask on
[Stack Overflow](https://stackoverflow.com).

If you are reporting a bug, please help to speed up problem diagnosis by providing as much information as
possible. Ideally, that would include a small sample project that reproduces the problem.

## Reporting Security Vulnerabilities

If you think you have found a security vulnerability in R2DBC MySQL please *DO NOT* disclose it publicly until
we've had a chance to fix it.

Please don't report security vulnerabilities using GitHub issues, because it is visible to everyone. Instead,
head over to [Security Policy][security-policy] and learn how to disclose them responsibly.

## Code Conventions and Housekeeping

None of these is essential for a pull request, but they will all help. They can also be added after the
original pull request but before a merge.

- Make sure all new `.java` files have a Javadoc class comment, and preferably at least a paragraph on what
  the class is for.
- Make sure your code is formatted. Recommended to use 4 spaces as indentation and continuous indentation. In
  addition, `intellij-style.xml` may help you format your code. See also [JetBrains IntelliJ IDEA][idea].
- All public methods/fields **available to users** should have a Javadoc method/field comment with tags that
  the method/field should have. For example:
  - `@return` if method would return a value.
  - `@param` each parameter description if method has least one parameter.
  - `@throws` the cause description of each exception if method would throw exception(s) in some cases.
  - `@since` the earliest version of the method/field has been added.
  - `@see` related class, method or link.
- Add the Apache License header comment to all new `.java` files (copy from existing files in the project).
- A few unit tests would help a lot as well -- someone has to do it.
- If no-one else is using your branch, please rebase it against the current `main` branch.
- When writing a commit message please follow [these conventions][commit-convention].
  - Recommended verbs: Add, Correct, Move, Fix, Upgrade, Polishing, Replace, Refactor, Remove, etc.
  - Not recommended verbs: Commit, Merge, Revert, Rebase or other git command keywords.
- Please use English for commit messages, code comments, pull requests, issue tickets and public discussions.

[security-policy]: https://github.com/mirromutth/r2dbc-mysql/security/policy
[idea]: https://www.jetbrains.com/idea
[commit-convention]: https://tbaggery.com/2008/04/19/a-note-about-git-commit-messages.html
