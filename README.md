# ByteCredits

Flask + SQLite: student wallets, NFC card registration, **add credits** (screenshot + admin approval), and **NFC payments** to another roll number.

## Run

```bash
pip install -r requirements.txt
python app.py
```

Open http://127.0.0.1:5000

## Roles

| Role    | How they get an account | After login |
|--------|---------------------------|-------------|
| Student | Self-register on **/register** | Student dashboard (NFC card setup if needed) |
| Teacher | Admin creates them on **/admin** (Staff ID + password) | Teacher dashboard |
| Admin   | Same form as teacher, role **Admin**, or use **/admin/login** with env password | Admin dashboard |

## Admin (approve credit top-ups & add teachers)

Open **http://127.0.0.1:5000/admin/login**

**Sample password (default, if you did not set an env var):** `bytecredits-admin`

For production, set your own password (overrides the sample):

```bash
set BYTECREDITS_ADMIN_PASSWORD=your-strong-password
python app.py
```

Use **Add teacher or admin account** to create staff logins (they sign in at **/login** with Staff ID). Approve or reject student credit screenshots as before.

**Classes & attendance:** Create a class with year, dept/section (forms the key `year_section`, e.g. `3_cse-a`), roll **prefix**, **start/end** sequence numbers, optional **missing** numbers, and optional **pad width**. An Excel roster is saved as `static/uploads/class_rosters/<key>.xlsx`. Teachers only see these classes on **Take attendance**. For a given **class + period + session date**, attendance can be submitted **once**; starting again the same day shows a lock message. Each successful submit also writes `static/uploads/attendance_exports/<class>_<YYYY-MM-DD>_P<period>_<subject>.xlsx` (worksheet tab = that date; the third column header is the **subject name**).

Optional: `SECRET_KEY` for stable session cookies across restarts.

## NFC payment flow

1. Enter **recipient roll number** and **amount**.
2. Enter the **payer’s 4-digit card PIN** (set when registering the card).
3. Choose **Phone NFC** or **USB / manual UID**, then **Pay — tap card**.
4. Server debits the card holder and credits the recipient if balance is sufficient.
