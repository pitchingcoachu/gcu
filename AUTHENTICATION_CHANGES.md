# Authentication Changes Summary

## Overview
Switched from custom shinymanager authentication to Shinyapps.io native authentication.

## Changes Made

### 1. Removed Custom Authentication (shinymanager)
- **Removed**: All shinymanager database setup code
- **Removed**: `secure_app()` UI wrapper
- **Removed**: `secure_server()` authentication server
- **Removed**: Custom login page styling and UI elements
- **Removed**: Password reset functionality
- **Removed**: Token persistence in localStorage

### 2. Implemented Native Shinyapps.io Authentication

**Three-Tier Access System:**

1. **Admin** - Full access + admin features
   ```r
   admin_emails <- c("jgaynor@pitchingcoachu.com")
   ```

2. **Coaches** - Can see all data, no admin features
   ```r
   coach_emails <- c(
     "jgaynor@pitchingcoachu.com",
     "jared.s.gaynor@gmail.com",
     "banni17@yahoo.com",
     "adam.racine@aol.com",
     "Njcbaseball08@gmail.com"
   )
   ```
   **Note**: Add new coaches to this list when you add them in shinyapps.io

3. **Players** - See only their own data
   - Identified by email being in `lookup_table.csv` `Email` column
   - Automatically filtered to see only pitches where `Email` matches their login
   - No code changes needed - just add their email to `lookup_table.csv`

- **Updated**: Server function to use `session$user` for authentication:
  ```r
  user_email <- reactive({
    email <- session$user
    if (is.null(email) || !nzchar(email)) {
      return(NA_character_)
    }
    as.character(email)
  })
  
  is_admin <- reactive({
    u <- user_email()
    !is.na(u) && u %in% admin_emails
  })
  ```

### 3. Access Control - Three-Tier System

**1. Admin User**: 
- Email: `jgaynor@pitchingcoachu.com`
- Can see ALL data
- Has admin privileges (modify pitch types, etc.)

**2. Coach Users**:
- Emails listed in `coach_emails` in app.R
- Can see ALL data
- Cannot use admin features

**3. Player Users**:
- Any email in `lookup_table.csv` Email column
- Can only see data where Email matches their login
- Cannot use admin features

**Authentication**: Managed entirely by shinyapps.io's user management system

**Important**: 
- The `Email` column in `lookup_table.csv` contains player emails for data filtering
- Players automatically get restricted view when they log in
- Coaches need to be added to both shinyapps.io AND the `coach_emails` list in code

## How to Add New Users

### Adding a New Coach:
1. **In shinyapps.io**: Add their email to Users/Access
2. **In code**: Add their email to `coach_emails` list in `app.R` (around line 16865)
3. **Redeploy** the app

### Adding a New Player:
1. **In shinyapps.io**: Add their email to Users/Access  
2. **In lookup_table.csv**: Add a row with their PlayerName and Email
3. **Redeploy** the app
   - No code changes needed - they'll automatically see only their data

### Making Someone an Admin:
1. Add their email to `admin_emails` in `app.R` (around line 16861)
2. Redeploy the app

## Admin Privileges
Only admin users can:
- Modify pitch type classifications
- Access certain administrative features
- See admin-only UI elements

Coaches can:
- View all data (all players, all sessions)
- Use all data visualization and analysis tools
- Access all suites (Pitching, Hitting, Catching, etc.)
- Create custom reports
- Add notes

Players can:
- View only their own data
- Use all visualization and analysis tools for their data
- Access all suites (with their data only)
- Create custom reports for their data
- Add notes about their own performance

## Files Changed
- `app.R`: Removed shinymanager code, simplified authentication logic
- `credentials.sqlite`: No longer needed (can be deleted)

## Benefits of This Change
1. **Simpler codebase**: Removed ~500 lines of authentication code
2. **Better security**: Uses Shinyapps.io's enterprise-grade authentication
3. **Easier user management**: Add/remove users through shinyapps.io dashboard
4. **No password management**: Users manage their own shinyapps.io accounts
5. **Single sign-on**: Users who already have shinyapps.io accounts can use them

## Testing Notes
When running locally (not on shinyapps.io), `session$user` will be NULL, so features requiring authentication won't work. This is expected - the app is designed to run on shinyapps.io with their authentication enabled.
