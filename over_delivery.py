# Import libraries

import os
import datetime
import pandas as pd
from sqlalchemy import create_engine, text
import fabory_logging
from sys import exit

# Get the current date and time
now = datetime.datetime.now()

"""
Formatting current date to variable 'current_date' as a string in the format dd.mm.yyyy
Variable 'current_date' is based on the date in the file name
"""
current_date = now.strftime("%d.%m.%Y")

# Define the path to the folder with all Fabory users
users_excel_file_path = r"/Path/To/File"

# Define the path to the folder with all copy recipients
copy_recipients_excel = r"/Path/To/File"

# Define the path to the exports folder
folder_path = "/Path/To/Folder"

# Setting file name with the current date (f.e. RPA_PO_OVERDEL_MK3_RPA_01.01.2000.xlsx)
file_name = f"FileName"

# Server connection details for the EOL_app database
_SERVER_ = ''    # Server IP or hostname
_DATABASE_ = ''  # Database name
_USERNAME_ = ''  # Database username
_PASSWORD_ = ''  # Database password
_PORT_ = ""      # Port for database communication

# Construct the connection string for the database
database_connection = f"SetDatabaseConnection"

def get_copy_recipients():
    """
    Reads a list of copy recipients from an Excel file and returns their email addresses as a single string.

    This function reads an Excel file containing email addresses, extracts them, and joins them into a single string
    separated by semicolons. It is intended to be used in scenarios where you need to collect email addresses of copy
    recipients, e.g., for notifications or distribution lists.

    Returns:
        str: A semicolon-separated string of copy recipient email addresses.
    """
    copy_recipients = pd.read_excel(copy_recipients_excel).values.tolist()

    # Extract the email addresses from the Excel data
    copy_recipients = [email_address[0] for email_address in copy_recipients]

    # Join the email addresses into a single string separated by semicolons
    copy_recipients_addresses = '; '.join(copy_recipients)

    return copy_recipients_addresses

def check_user_id_exists():
    """
    This function reads user data from the specified Excel file and extracts user IDs and email addresses.
    It uses pandas to read the data and list comprehensions to extract the required information.

    Returns:
    A tuple with two lists: one containing user IDs and another containing email addresses.
    """

    # Read user data from the Excel file
    data_frame_users = pd.read_excel(users_excel_file_path)

    # Convert user data to a list of lists
    user_data = data_frame_users.values.tolist()

    # Extract user IDs and email addresses from the user data
    ids = [id[0] for id in user_data]
    addresses = [email_address[1] for email_address in user_data]

    # Return a list containing user IDs and email addresses
    return ids, addresses


# Function to prepare and send email data to users who are in excel file
def get_email_data_for_user(log, unique_users, filtered_data, connection):

    """
    This function prepares and sends email data to users who are present in the Excel file.
    It retrieves user IDs and email addresses, filters data for each unique user, and sends an email to them.

    Parameters:
    - unique_users: List of unique users for which emails need to be sent.
    - filtered_data: Data frame containing the filtered data from the Excel file.
    - connection: Database connection instance for sending emails.
    """

    # Retrieve user IDs and email addresses using the check_user_id_exists() function
    user_ids, email_addresses = check_user_id_exists()

    # Retrieve copy recipients' email addresses
    copy_recipients = get_copy_recipients()

    for user in unique_users:

        # Filter data for the current user
        user_data = filtered_data[filtered_data['ColumnName'] == user]

        # Convert user data to HTML format, replacing single quotes with special characters
        html_data = user_data.to_html().replace("'", "´")

        # Check if the HTML data is empty or the user is 'BATCHPD'
        if html_data == "" or user[:5] == "Value":

            continue  # Skip empty data or specific user

        else:

            # Check if the user ID exists in the list of user IDs
            if user in user_ids:

                # If the user ID exists, find its index in the list
                index = user_ids.index(user)

                # Retrieve the corresponding email address using the index
                email_address = email_addresses[index]
            else:

                # If the user ID doesn't exist, set a default email address
                email_address = "EmailAddress"

        # Compose the email content and send the email
        html_data = "<p>Text_Which_Is_In_Email_Body</p>" + html_data
        send_mail(log, user, email_address, copy_recipients, html_data, file_name, connection)


# Define a function for handling error notifications via email
def error_handling(msg, connection):

    """
    This function handles errors by sending error notifications via email.
    It constructs and executes a SQL query to send an email using SQL Server's sp_send_dbmail stored procedure.

    Parameters:
    - msg: Error message string that needs to be sent in the email.
    - connection: Database connection instance for executing the query.
    """

    # Construct the query to send an email using the SQL Server's sp_send_dbmail stored procedure
    query = f"""SQL_QUERY"""
    try:

        # Execute the query and send an email using the connection to the database
        data = pd.read_sql_query(text(query), con=connection)
        connection.commit()

    except Exception as error:

        # If an error occurs, ensure the transaction is committed to the database
        connection.commit()


def send_mail(log, user, email_address, copy_recipients, html_data, attachment, connection):

    """
    This function constructs a SQL query to send an email with the specified content and attachment.
    It uses SQL Server's sp_send_dbmail stored procedure to send the email and logs the action.

    Parameters:
    - user: Recipient.
    - email_address: Recipient's email address.
    - html_data: The main content of the email.
    - attachment: Name of the file to be attached to the email.
    - connection: Database connection instance for executing the query.
    """

    # Construct the query to send an email using the SQL Server's sp_send_dbmail stored procedure
    query = f"""SQL_QUERY"""
    try:

        # Execute the query to send the email and commit changes to the connection
        data = pd.read_sql_query(text(query), con=connection)
        connection.commit()

    except Exception as error:

        # In case of an exception, commit changes to the connection to ensure data consistency
        connection.commit()

    log.info(f"Information_That_Email_Was_Sent")


def main():

    """
    Main function of the script.
    It constructs the file path, checks if it exists, reads the data, filters it, and initiates email sending to users.
    """

    # Initialize a logger using fabory_logging module
    log = fabory_logging.fabory_logger(__name__)

    # Create a database engine using the connection string
    engine = create_engine(database_connection)

    # Log a debug message to indicate the start of a script
    log.debug("Starting a script.")

    try:

        # Establish a connection to the database using the engine
        connection = engine.connect()

        # Construct the full file path by joining the folder path and file name
        file_path = os.path.join(folder_path, file_name)

        # Check if the file path exists
        if not os.path.exists(file_path):

            # If the file path doesn't exist, generate an error message and send an error notification
            msg = f"Path: {file_path} was not found."
            error_handling(msg, connection)

            # Log an error message to indicate that the file path was not found
            log.error(f"Path: {file_path} was not found.")
            exit()

        else:

            # If the file path exists:
            # Read the Excel file into a DataFrame
            data_frame = pd.read_excel(file_path, dtype=str)

            # Save the DataFrame as an Excel file at a specified network path, excluding the index
            data_frame.to_excel(f"/Saving/Excel/To/Server/Path", index=False)

            # Filter the data to retain only rows where the 'OVERDELIVERY' column has the value 'Y'
            filtered_data = data_frame[data_frame['ColumnName'] == 'Value']

            # Check if the filtered data is empty
            if filtered_data.empty:

                log.info("Information_That_Data_Frame_Is_Empty")
                exit()

            else:

                # Get unique users from the filtered data
                unique_users = filtered_data['ColumnName'].unique()

                # Logging the process of retrieving email data for users.
                log.info(f"Information: {unique_users}")

                # Setting email data for all users in the excel file
                get_email_data_for_user(log, unique_users, filtered_data, connection)

    except Exception as error:

        # If a connection error occurs, log an error message
        log.error("Connection to database failed!")

# Entry point of the script
if __name__ == "__main__":

    # Call the main function to start the script's execution
    main()