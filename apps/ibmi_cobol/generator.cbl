        IDENTIFICATION DIVISION.
        PROGRAM-ID. GENERATOR.
        ENVIRONMENT DIVISION.
        CONFIGURATION SECTION.
        INPUT-OUTPUT SECTION.
        FILE-CONTROL.
            SELECT BALANCE-FILE ASSIGN TO DATABASE-BALFILE.
        DATA DIVISION.
        FILE SECTION.
        FD  BALANCE-FILE.
        01  BALANCE-RECORD.
            05  BR-ACCOUNT-ID         PIC X(10).
            05  BR-SEPARATOR-1        PIC X(01).
            05  BR-BALANCE            PIC 9(08)V99.
            05  BR-SIGN               PIC X(01).
            05  BR-CURRENCY           PIC X(03).
            05  BR-TIMESTAMP          PIC 9(12).
            05  FILLER                PIC X(43).
        WORKING-STORAGE SECTION.
        01  WS-TOTAL-RECORDS          PIC 9(07) VALUE 1000.
        01  WS-IDX                    PIC 9(07).
        01  WS-TEMP-ID.
            05  FILLER                PIC X(4) VALUE 'ACC-'.
            05  WS-ID-NUM             PIC 9(06).
        PROCEDURE DIVISION.
        0000-MAIN.
            OPEN OUTPUT BALANCE-FILE.
            PERFORM 3000-GENERATE-AND-WRITE
                VARYING WS-IDX FROM 1 BY 1
                UNTIL WS-IDX > WS-TOTAL-RECORDS.
            CLOSE BALANCE-FILE.
            STOP RUN.
        3000-GENERATE-AND-WRITE.
            MOVE SPACES TO BALANCE-RECORD.
            MOVE WS-IDX TO WS-ID-NUM.
            MOVE WS-TEMP-ID TO BR-ACCOUNT-ID.
            MOVE "+" TO BR-SEPARATOR-1.
            COMPUTE BR-BALANCE = FUNCTION RANDOM * 1000000
            WRITE BALANCE-RECORD.