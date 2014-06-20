# Contributing

We want to make it as easy as possible for people to contribute cool new things to Cayley.

## I want to build the community!

This is, really, a community project and community is important. Write to the mailing list, ask and answer questions,
and generally being involved in growing the project is, perhaps, the most important contribution.

## I found a bug!

Opening an issue on the issue tracker is the most helpful thing. However, some notes:

* Check the issue tracker and mailing list to see if it's already a bug.
* Include as many details in your bug as possible. Finding your log file (often in /tmp, but can be
    controlled with command line flags) can be a great help.
* Often a bug comes with unexpected behavior for a query. *Please include the query* and how you found the bug.

## I want to contribute a pull request!

First things first:

**Sign the Google [Contributor License Agreement][cla]** (you can sign online at the bottom of that page). 

You _must_ sign this form, otherwise we cannot merge in your changes. 
**_Always_ mention in the pull request that you've signed it**, even 
if you signed it for a previous pull request (you only need to sign the CLA once).

If you're contributing on behalf of your company, please email the mailing list and look into signing
Google's [Corporate Contributor License Agreement][cla-corp].

### I'm not much of a coder, but I want to help!

Documentation is always important. 
If you want to make a passage clearer, expand on the implications, write about schema and best practices, these are just as helpful
as code.

Just running Cayley and coming up with benchmarks and performance numbers in docs and on the mailing list is also helpful.

### I want to extend Cayley in code, but I don't know what's needed?

Check out TODO.md for random thoughts that authors have contributed. Usually if it's agreed that it's a good 
idea in the mailing list, in chat or elsewhere, a pull request to add to TODO.md is perfectly fine.

If you want to start simple, find a place to improve test coverage. The coverage metrics are part of "make test" and anything that
improves testing is good news.

If you're a designer, most of the work on Cayley to date has been on the backend -- improving the UI is certainly helpful!

### I want to (interface with a backend/write a new query language/add some other cool feature).

Please do, and send the pull request. It may or may not get in as-is, but it will certainly generate discussion to see a working version. 
Obviously, well-tested and documented code makes a stronger case.

It's a general philosophy that, if it's modular and doesn't break things, it's generally a good thing to have! 
The worst that will happen is it will go unused and possibly become deprecated, and ultimately removed by the maintainers. So if you have
a pet project, make sure it's either something you'll maintain or has enough of a user base to stick around.


[cla]: https://developers.google.com/open-source/cla/individual
[cla-corp]:https://developers.google.com/open-source/cla/corporate
