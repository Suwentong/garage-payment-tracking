from fastapi import APIRouter, UploadFile, File, HTTPException
from fastapi.responses import FileResponse
import pandas as pd
import tempfile
import os
from pathlib import Path
from datetime import datetime
import uuid

from app.services.file_reader_service import FileReaderService
from app.services.payment_tracker_service import PaymentTrackerService

router = APIRouter()


@router.post("/generate")
async def generate_report(
    rent_file: UploadFile = File(..., description="Rent file (Excel format)"),
    bank_statement_file: UploadFile = File(
        ..., description="Bank statement file (Excel format)"
    ),
):
    """
    Generate payment status report from uploaded files.

    Args:
        rent_file: Excel file containing garage rental information
        bank_statement_file: Excel file containing bank statement/payment information

    Returns:
        Excel file with payment status report
    """
    try:
        # Validate file types
        if not rent_file.filename.endswith((".xlsx", ".xls")):
            raise HTTPException(
                status_code=400, detail="Rent file must be Excel format (.xlsx or .xls)"
            )

        if not bank_statement_file.filename.endswith((".xlsx", ".xls")):
            raise HTTPException(
                status_code=400,
                detail="Bank statement file must be Excel format (.xlsx or .xls)",
            )

        # Create temporary files
        with tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx") as rent_temp:
            rent_content = await rent_file.read()
            rent_temp.write(rent_content)
            rent_temp_path = rent_temp.name

        with tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx") as bank_temp:
            bank_content = await bank_statement_file.read()
            bank_temp.write(bank_content)
            bank_temp_path = bank_temp.name

        try:
            # Initialize file reader service
            file_service = FileReaderService()

            # Read and process files
            file_service.read_rent_file(rent_temp_path)
            file_service.read_bank_statement_file(bank_temp_path)

            processed_rent = file_service.process_rent_data()
            processed_bank = file_service.process_bank_statement_data()

            # Initialize payment tracker service
            tracker_service = PaymentTrackerService()
            tracker_service.set_data(processed_rent, processed_bank)

            # Generate payment status report
            payment_report = tracker_service.generate_payment_report()

            # Generate final report
            report_id = str(uuid.uuid4())
            report_path = Path(f"/tmp/payment_report_{report_id}.xlsx")

            # Export comprehensive report
            with pd.ExcelWriter(report_path, engine="openpyxl") as writer:
                # Payment status report (main sheet)
                payment_report.to_excel(
                    writer, sheet_name="Отчет по платежам", index=False
                )

                # Summary statistics
                summary = tracker_service.get_payment_summary()
                summary_df = pd.DataFrame([summary])
                summary_df.to_excel(writer, sheet_name="Сводка", index=False)

                # Original processed data (for reference)
                processed_rent.to_excel(writer, sheet_name="Данные аренды", index=False)
                processed_bank.to_excel(
                    writer, sheet_name="Банковские данные", index=False
                )

            # Return the file
            return FileResponse(
                path=report_path,
                filename=f"payment_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )

        finally:
            # Clean up temporary files
            os.unlink(rent_temp_path)
            os.unlink(bank_temp_path)

    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Error generating report: {str(e)}"
        )
