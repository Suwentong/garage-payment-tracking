import pandas as pd
from datetime import datetime
import logging
import re
from typing import Optional, Dict, Tuple

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

    def _detect_rent_columns(self, df: pd.DataFrame) -> Dict[str, str]:
        """
        Dynamically detect column mappings for rent data.

        Args:
            df: DataFrame with rent data

        Returns:
            Dictionary mapping detected columns to standard names
        """
        column_mapping = {}
        columns = list(df.columns)

        # Possible column names for each field (case-insensitive)
        garage_names = [
            "гараж",
            "garage",
            "номер гаража",
            "garage number",
            "garage_name",
        ]
        amount_names = [
            "сумма",
            "amount",
            "payment_amount",
            "сумма оплаты",
            "payment amount",
        ]
        date_names = [
            "дата",
            "date",
            "payment_date",
            "дата оплаты",
            "первоначальная дата",
            "initial date",
        ]

        # Detect garage column
        for col in columns:
            col_lower = str(col).lower()
            if any(name in col_lower for name in garage_names):
                column_mapping["garage_name"] = col
                break

        # Detect amount column
        for col in columns:
            col_lower = str(col).lower()
            if any(name in col_lower for name in amount_names):
                column_mapping["payment_amount"] = col
                break

        # Detect date column
        for col in columns:
            col_lower = str(col).lower()
            if any(name in col_lower for name in date_names):
                column_mapping["payment_date"] = col
                break

        # Log detected mappings
        logger.info(f"Detected column mappings: {column_mapping}")

        return column_mapping

    def _detect_bank_statement_columns(
        self, df: pd.DataFrame
    ) -> Tuple[Optional[int], Optional[int], Optional[int]]:
        """
        Dynamically detect column positions for bank statement data.

        Args:
            df: DataFrame with bank statement data

        Returns:
            Tuple of (date_column_index, amount_column_index, description_column_index)
        """
        date_col_idx = None
        amount_col_idx = None
        description_col_idx = None

        # Sample some rows to detect patterns
        sample_rows = df.head(50)

        # Detect date column by looking for date patterns
        date_pattern = r"\d{2}\.\d{2}\.\d{4}"
        for col_idx in range(len(df.columns)):
            col_data = sample_rows.iloc[:, col_idx].astype(str)
            date_matches = col_data.str.contains(date_pattern, na=False)
            if date_matches.sum() > 0:
                date_col_idx = col_idx
                logger.info(f"Detected date column at index {col_idx}")
                break

        # Detect amount column by looking for numeric patterns with +/-
        # Look for patterns like "+2 750,00" or "-1 200,00"
        amount_pattern = r"[+-]?\d+[\s,]*\d*[,.]?\d*"
        best_amount_col = None
        best_amount_score = 0

        for col_idx in range(len(df.columns)):
            if col_idx == date_col_idx:
                continue

            col_data = sample_rows.iloc[:, col_idx].astype(str)
            amount_matches = col_data.str.contains(amount_pattern, na=False)

            # Calculate a score based on:
            # 1. Number of matches
            # 2. Whether the values look like currency amounts (not just small numbers)
            # 3. Presence of + or - signs
            score = amount_matches.sum()

            if score > 0:
                # Check if values look like currency amounts
                matched_values = col_data[amount_matches]
                currency_like = 0
                has_signs = 0

                for val in matched_values:
                    # Check if value looks like currency (has thousands separator or decimal)
                    if re.search(r"[\s,]\d{3}|[,.]\d{2}", val):
                        currency_like += 1
                    # Check if value has + or - sign
                    if re.search(r"^[+-]", val.strip()):
                        has_signs += 1

                # Adjust score based on currency-like patterns
                if currency_like > 0:
                    score += currency_like * 2
                if has_signs > 0:
                    score += has_signs

                if score > best_amount_score:
                    best_amount_score = score
                    best_amount_col = col_idx

        if best_amount_col is not None:
            amount_col_idx = best_amount_col
            logger.info(
                f"Detected amount column at index {best_amount_col} (score: {best_amount_score})"
            )

        # Detect description column (usually the second column or one with text)
        for col_idx in range(len(df.columns)):
            if col_idx != date_col_idx and col_idx != amount_col_idx:
                col_data = sample_rows.iloc[:, col_idx].astype(str)
                # Look for columns with text content (not just numbers or dates)
                text_content = col_data.str.len() > 0
                if text_content.sum() > 0:
                    description_col_idx = col_idx
                    logger.info(f"Detected description column at index {col_idx}")
                    break

        return date_col_idx, amount_col_idx, description_col_idx

    def process_rent_data(self) -> pd.DataFrame:
        """
        Process the rent data to standardize column names and format.

        Returns:
            Processed DataFrame with standardized columns
        """
        if self.rent_data is None:
            raise ValueError("Rent data not loaded. Call read_rent_file first.")

        df = self.rent_data.copy()

        # Detect column mappings dynamically
        column_mapping = self._detect_rent_columns(df)

        # Validate that we found all required columns
        required_columns = ["garage_name", "payment_amount", "payment_date"]
        missing_columns = [col for col in required_columns if col not in column_mapping]

        if missing_columns:
            raise ValueError(
                f"Could not detect required columns: {missing_columns}. "
                f"Available columns: {list(df.columns)}"
            )

        # Rename columns using the reverse mapping (original name -> standard name)
        reverse_mapping = {v: k for k, v in column_mapping.items()}
        df = df.rename(columns=reverse_mapping)

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

        # Detect column positions dynamically
        date_col_idx, amount_col_idx, desc_col_idx = (
            self._detect_bank_statement_columns(df)
        )

        if date_col_idx is None:
            raise ValueError("Could not detect date column in bank statement")

        if amount_col_idx is None:
            raise ValueError("Could not detect amount column in bank statement")

        # Find rows with dates
        date_pattern = r"\d{2}\.\d{2}\.\d{4}"
        date_rows = df[
            df.iloc[:, date_col_idx].astype(str).str.contains(date_pattern, na=False)
        ]

        if date_rows.empty:
            logger.warning("No date rows found in bank statement")
            return pd.DataFrame()

        # Extract payment information
        payments = []
        for idx, row in date_rows.iterrows():
            try:
                # Get date from detected column
                date_str = str(row.iloc[date_col_idx]).strip()
                # Extract date from the string (it might have extra text)
                date_match = re.search(date_pattern, date_str)
                if date_match:
                    date_str = date_match.group()
                    payment_date = datetime.strptime(date_str, "%d.%m.%Y").date()

                    # Get amount from detected column
                    amount_str = str(row.iloc[amount_col_idx])
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
                                # Get description from detected column or empty string
                                description = ""
                                if desc_col_idx is not None:
                                    description = str(row.iloc[desc_col_idx])

                                payments.append(
                                    {
                                        "payment_date": payment_date,
                                        "amount": amount,
                                        "description": description,
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
