The data in the above Streamlit app is not stored in a traditional database like MySQL or PostgreSQL. Instead, it uses Excel files as the storage mechanism within a local directory (data/ folder).
Here’s how the storage works:


History of uploads → stored in history.xlsx
(Tracks file metadata: id, filename, saved_path, uploader, upload_dt, reporting_month, rows_count, status, active, superseded_by, validation_status)


Monthly Scorecard Data → stored in combined_data.xlsx
(Contains all rows from the "Data" sheet of uploaded files)


YTD Data → stored in combined_ytd.xlsx
(Contains all rows from the "YTD" sheet of uploaded files)


Audit Log → stored in audit_log.xlsx
(Tracks deletion and invalidation actions)


Uploaded files → saved in data/attachments/ directory.

How it works (quick recap)


Team leads go to “Team Lead Monthly Comments”:

Pick the month (default to latest active).
Select Domain ID(s) (we build a master list from your combined data, including optional domain names).
Enter a comment (applies to all selected domains).
Save (each domain gets its own comment row with an audit‑friendly comment_id).
Manage (edit/delete) their own comments. Admins can manage all.



Viewers (and other roles) go to “My Monthly Comments”:

See only comments where domain_id ∈ USERS[username]['domain_ids'].
Download their visible comments.



Extra visibility: On Monthly Scorecard Dashboard, there’s an expander “Domain Comments (visible to you)” showing the same restricted set under the page’s month filter.