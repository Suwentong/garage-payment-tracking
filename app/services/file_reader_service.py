import pandas as pd
from datetime import datetime
import logging
import re

logger = logging.getLogger(__name__)


class FileReaderService:
    def __init__(self):
        self.rent_data = None
        self.bank_statement_data = None
        self.processed_rent_data = None
        self.processed_bank_data = None

    def read_rent_file(self, file_path: str) -> pd.DataFrame:
        """
        Read the rent file containing garage rental information.

        Args:
            file_path: Path to the rent file

        Returns:
            DataFrame with garage rental data
        """
        try:
            df = pd.read_excel(file_path)
            logger.info(f"Successfully read rent file: {file_path}")
            logger.info(f"Columns found: {list(df.columns)}")
            logger.info(f"Data shape: {df.shape}")

            # Store the raw data
            self.rent_data = df
            return df

        except Exception as e:
            logger.error(f"Error reading rent file {file_path}: {str(e)}")
            raise

    def read_bank_statement_file(self, file_path: str) -> pd.DataFrame:
        """
        Read the bank statement file (print2.xlsx) containing payment information.

        Args:
            file_path: Path to the bank statement file

        Returns:
            DataFrame with bank statement data
        """
        try:
            df = pd.read_excel(file_path)
            logger.info(f"Successfully read bank statement file: {file_path}")
            logger.info(f"Columns found: {list(df.columns)}")
            logger.info(f"Data shape: {df.shape}")

            # Store the raw data
            self.bank_statement_data = df
            return df

        except Exception as e:
            logger.error(f"Error reading bank statement file {file_path}: {str(e)}")
            raise

    def process_rent_data(self) -> pd.DataFrame:
        """
        Process the rent data to standardize column names and format.

        Returns:
            Processed DataFrame with standardized columns
        """
        if self.rent_data is None:
            raise ValueError("Rent data not loaded. Call read_rent_file first.")

        df = self.rent_data.copy()

        # Map actual column names to expected names
        column_mapping = {
            "Гараж": "garage_name",
            "Сумма": "payment_amount",
            "Первоначальная дата": "payment_date",
        }

        # Rename columns
        df = df.rename(columns=column_mapping)

        # Ensure payment_date is datetime
        df["payment_date"] = pd.to_datetime(df["payment_date"]).dt.date

        # Add tenant name column (placeholder for now)
        df["tenant_name"] = f"Арендатор гаража"

        # Store processed data
        self.processed_rent_data = df

        logger.info(f"Processed rent data shape: {df.shape}")
        logger.info(f"Processed columns: {list(df.columns)}")

        return df

    def process_bank_statement_data(self) -> pd.DataFrame:
        """
        Process the bank statement data to extract payment information.

        Returns:
            Processed DataFrame with payment records
        """
        if self.bank_statement_data is None:
            raise ValueError(
                "Bank statement data not loaded. Call read_bank_statement_file first."
            )

        df = self.bank_statement_data.copy()

        # Find the header row (usually contains date and amount information)
        # Look for rows that contain date patterns
        date_pattern = r"\d{2}\.\d{2}\.\d{4}"

        # Find rows with dates
        date_rows = df[df.iloc[:, 0].astype(str).str.contains(date_pattern, na=False)]

        if date_rows.empty:
            logger.warning("No date rows found in bank statement")
            return pd.DataFrame()

        # Extract payment information
        payments = []
        for idx, row in date_rows.iterrows():
            try:
                # First column should contain the date
                date_str = str(row.iloc[0]).strip()
                # Extract date from the string (it might have extra text)
                date_match = re.search(date_pattern, date_str)
                if date_match:
                    date_str = date_match.group()
                    payment_date = datetime.strptime(date_str, "%d.%m.%Y").date()

                    # Look for amount in the last column
                    amount_str = str(row.iloc[-1])
                    # Extract numeric value from amount string (handle + and - signs)
                    amount_match = re.search(r"[+-]?[\d\s,]+", amount_str)
                    if amount_match:
                        amount_str = (
                            amount_match.group().replace(" ", "").replace(",", ".")
                        )
                        try:
                            amount = float(amount_str)
                            # Only include positive amounts (incoming payments)
                            if amount > 0:
                                payments.append(
                                    {
                                        "payment_date": payment_date,
                                        "amount": amount,
                                        "description": (
                                            str(row.iloc[1]) if len(row) > 1 else ""
                                        ),
                                    }
                                )
                        except ValueError:
                            continue
            except Exception as e:
                logger.warning(f"Error processing row {idx}: {e}")
                continue

        processed_df = pd.DataFrame(payments)

        # Store processed data
        self.processed_bank_data = processed_df

        logger.info(f"Processed bank statement data shape: {processed_df.shape}")
        if not processed_df.empty:
            logger.info(f"Processed columns: {list(processed_df.columns)}")

        return processed_df
