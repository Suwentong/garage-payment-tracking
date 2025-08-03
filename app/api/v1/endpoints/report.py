from fastapi import APIRouter, UploadFile, File, HTTPException
from fastapi.responses import FileResponse
import pandas as pd
import tempfile
import os
from pathlib import Path
from datetime import datetime
import uuid

from app.services.file_reader_service import FileReaderService

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
            service = FileReaderService()

            # Read and process files
            rent_df = service.read_rent_file(rent_temp_path)
            bank_df = service.read_bank_statement_file(bank_temp_path)

            processed_rent = service.process_rent_data()
            processed_bank = service.process_bank_statement_data()

            # Generate report
            report_id = str(uuid.uuid4())
            report_path = Path(f"/tmp/payment_report_{report_id}.xlsx")

            # Create simple report with processed data
            with pd.ExcelWriter(report_path, engine="openpyxl") as writer:
                processed_rent.to_excel(writer, sheet_name="Rent Data", index=False)
                processed_bank.to_excel(writer, sheet_name="Bank Data", index=False)

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
