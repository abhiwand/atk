Misc Notes
==========

uh, this was a thought once --something about not cancelling the job on an error, but just marking row/cell as None and reporting ``raise FillNone("col value out of range")`` map or whatever will catch this, log it, add to a count in the report, and fill the entry with a None

