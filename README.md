# Tutorial

[tutorials.ipynb](tutorials.ipynb): [![run it online](https://mybinder.org/badge_logo.svg)](https://mybinder.org/v2/gh/agy-why/rave-tap-pyvo-tutorials/master?filepath=tutorials.ipynb)

# Examples 

A few scripts to access data via the TAP interface of `rave-survey.org` with pyvo

* [rave-tutorial-sync-job.py](rave-tutorial-sync-job.py) : submit and retrieve a short query
* [rave-tutorial-async-60s.py](rave-tutorial-async-60s.py) : submit and retrieve an asynchrone query to the 60 seconds queue
* [rave-tutorial-submit-30m.py](rave-tutorial-submit-30m.py) : submit an asynchrone query to the 30 minutes queue
* [rave-tutorial-retrieve-30m.py](rave-tutorial-retrieve-30m.py) : retrieve an asynchrone query from the 30 minutes queue
* [rave-tutorial-submit-multi.py](rave-tutorial-submit-multi.py) : submit chunked queries
* [rave-tutorial-retrieve-multi.py](rave-tutorial-retrieve-multi.py) : retrieve chunked queries
* [rave-tutorial-from-files.py](rave-tutorial-from-files.py) : submit and retrieve multiple queries from files
* [rave-tutorial-download-files.py](rave-tutorial-download-files.py) : automatically download file from query
* [rave-tutorial-archive-jobs.py](rave-tutorial-archive-jobs.py) : archive completed jobs
* [rave-tutorial-rerunning-archived-jobs.py](rave-tutorial-rerunning-archived-jobs.py) : resubmit and retrieve archived jobs

## Usage

replace the `<your-token>` with your authentification Token for `rave-survey.org` and run the
scripts as `python <script>.py`.

## Remarks

The print statements in the tutorials are only for education purposes.

## Contributions

* Yori Fournier
* Harry Enke
* Anastasia Galkin
* Arman Khalatyan

## LICENCE

Public Domain - CC0