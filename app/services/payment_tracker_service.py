import pandas as pd
from datetime import date
from typing import Dict, Optional
import calendar
import logging

logger = logging.getLogger(__name__)


class PaymentStatus:
    """Payment status enumeration."""

    RECEIVED = "получен"
    OVERDUE = "просрочен"
    NOT_DUE = "срок не наступил"


class PaymentTrackerService:
    """
    Service for tracking garage rental payments and determining payment status.
    """

    def __init__(self):
        self.rent_data = None
        self.bank_data = None
        self.payment_report = None

    def set_data(self, rent_data: pd.DataFrame, bank_data: pd.DataFrame):
        """
        Set the processed rent and bank data for analysis.

        Args:
            rent_data: Processed rent data with garage information
            bank_data: Processed bank statement data with payments
        """
        self.rent_data = rent_data
        self.bank_data = bank_data
        logger.info(
            f"Payment tracker initialized with {len(rent_data)} rent records and {len(bank_data)} bank records"
        )

    def _adjust_payment_date(self, payment_date: date) -> date:
        """
        Adjust payment date according to business rules.
        If payment date is 31st (or 29th/30th for February) and month is shorter,
        use the last day of the month.

        Args:
            payment_date: Original payment date

        Returns:
            Adjusted payment date
        """
        year = payment_date.year
        month = payment_date.month
        day = payment_date.day

        # Get last day of the month
        last_day = calendar.monthrange(year, month)[1]

        # Check if adjustment is needed
        if day in [29, 30, 31] and day > last_day:
            adjusted_date = date(year, month, last_day)
            logger.info(f"Adjusted payment date from {payment_date} to {adjusted_date}")
            return adjusted_date

        return payment_date

    def _find_matching_payment(
        self,
        garage_name: str,
        expected_amount: float,
        expected_date: date,
        tolerance_days: int = 3,
    ) -> Optional[Dict]:
        """
        Find a matching payment in bank data for a specific garage.

        Args:
            garage_name: Name of the garage
            expected_amount: Expected payment amount
            expected_date: Expected payment date
            tolerance_days: Number of days tolerance for payment matching

        Returns:
            Matching payment record or None
        """
        if self.bank_data.empty:
            return None

        # Look for payments with matching amount
        matching_amount = self.bank_data[self.bank_data["amount"] == expected_amount]

        if matching_amount.empty:
            return None

        # Check if any payment is within tolerance period
        for _, payment in matching_amount.iterrows():
            payment_date = payment["payment_date"]

            # Check if payment is within tolerance days before or after expected date
            date_diff = abs((payment_date - expected_date).days)

            if date_diff <= tolerance_days:
                logger.info(
                    f"Found matching payment: {expected_amount} on {payment_date} for garage {garage_name}"
                )
                return {
                    "payment_date": payment_date,
                    "amount": payment["amount"],
                    "description": payment.get("description", ""),
                }

        return None

    def _determine_payment_status(
        self,
        expected_date: date,
        matching_payment: Optional[Dict],
        current_date: date = None,
    ) -> str:
        """
        Determine payment status based on expected date and actual payment.

        Args:
            expected_date: Expected payment date
            matching_payment: Actual payment record if found
            current_date: Current date for status calculation (defaults to today)

        Returns:
            Payment status string
        """
        if current_date is None:
            current_date = date.today()

        # If payment found, status is received
        if matching_payment is not None:
            return PaymentStatus.RECEIVED

        # Calculate days since expected date
        days_since_expected = (current_date - expected_date).days

        # If expected date is in the future, status is not due
        if days_since_expected < 0:
            return PaymentStatus.NOT_DUE

        # If more than 3 days have passed, status is overdue
        if days_since_expected > 3:
            return PaymentStatus.OVERDUE

        # Within 3 days, still not due (grace period)
        return PaymentStatus.NOT_DUE

    def generate_payment_report(self, current_date: date = None) -> pd.DataFrame:
        """
        Generate payment status report for all garages.

        Args:
            current_date: Current date for status calculation (defaults to today)

        Returns:
            DataFrame with payment status report
        """
        if self.rent_data is None or self.bank_data is None:
            raise ValueError("Rent and bank data must be set before generating report")

        if current_date is None:
            current_date = date.today()

        logger.info(
            f"Generating payment report for {len(self.rent_data)} garages as of {current_date}"
        )

        report_data = []

        for _, garage in self.rent_data.iterrows():
            garage_name = garage["garage_name"]
            expected_amount = garage["payment_amount"]
            expected_date = garage["payment_date"]
            tenant_name = garage["tenant_name"]

            # Adjust payment date if needed
            adjusted_date = self._adjust_payment_date(expected_date)

            # Find matching payment
            matching_payment = self._find_matching_payment(
                garage_name, expected_amount, adjusted_date
            )

            # Determine payment status
            status = self._determine_payment_status(
                adjusted_date, matching_payment, current_date
            )

            # Get actual payment date if payment was received
            actual_payment_date = None
            if matching_payment:
                actual_payment_date = matching_payment["payment_date"]

            # Create report entry
            report_entry = {
                "garage_name": garage_name,
                "expected_payment_date": adjusted_date,
                "payment_amount": expected_amount,
                "status": status,
                "tenant_name": tenant_name,
                "actual_payment_date": actual_payment_date,
            }

            report_data.append(report_entry)

            logger.debug(
                f"Garage {garage_name}: {status} (expected: {adjusted_date}, actual: {actual_payment_date})"
            )

        # Create DataFrame
        self.payment_report = pd.DataFrame(report_data)

        # Log summary
        status_counts = self.payment_report["status"].value_counts()
        logger.info(f"Payment report generated. Status summary: {dict(status_counts)}")

        return self.payment_report

    def get_payment_summary(self) -> Dict:
        """
        Get summary statistics of payment statuses.

        Returns:
            Dictionary with payment status summary
        """
        if self.payment_report is None:
            raise ValueError("Payment report must be generated first")

        status_counts = self.payment_report["status"].value_counts()

        summary = {
            "total_garages": len(self.payment_report),
            "received_payments": status_counts.get(PaymentStatus.RECEIVED, 0),
            "overdue_payments": status_counts.get(PaymentStatus.OVERDUE, 0),
            "not_due_payments": status_counts.get(PaymentStatus.NOT_DUE, 0),
            "status_breakdown": dict(status_counts),
        }

        return summary

    def export_report_to_excel(self, file_path: str) -> None:
        """
        Export payment report to Excel file.

        Args:
            file_path: Path where to save the Excel file
        """
        if self.payment_report is None:
            raise ValueError("Payment report must be generated first")

        with pd.ExcelWriter(file_path, engine="openpyxl") as writer:
            # Main payment report
            self.payment_report.to_excel(
                writer, sheet_name="Отчет по платежам", index=False
            )

            # Summary sheet
            summary = self.get_payment_summary()
            summary_df = pd.DataFrame([summary])
            summary_df.to_excel(writer, sheet_name="Сводка", index=False)

        logger.info(f"Payment report exported to {file_path}")

    def get_overdue_payments(self) -> pd.DataFrame:
        """
        Get list of overdue payments.

        Returns:
            DataFrame with overdue payments only
        """
        if self.payment_report is None:
            raise ValueError("Payment report must be generated first")

        overdue = self.payment_report[
            self.payment_report["status"] == PaymentStatus.OVERDUE
        ]
        return overdue
