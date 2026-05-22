        IDENTIFICATION DIVISION.
        PROGRAM-ID. READER.
        ENVIRONMENT DIVISION.
        INPUT-OUTPUT SECTION.
        FILE-CONTROL.
            SELECT BALANCE-FILE ASSIGN TO DATABASE-BALFILE.
        DATA DIVISION.
        FILE SECTION.
        FD  BALANCE-FILE.
        01  BALANCE-RECORD.
            05  BR-ACCOUNT-ID          PIC X(10).
            05  BR-SEP                 PIC X(01).
            05  BR-BALANCE             PIC 9(08)V99.
            05  BR-SIGN                PIC X(01).
            05  BR-CURRENCY            PIC X(03).
            05  BR-TIMESTAMP           PIC 9(12).
            05  FILLER                 PIC X(43).
        WORKING-STORAGE SECTION.
        01  WS-EOF-FLAG                PIC X VALUE 'N'.
        01  WS-FOUND-FLAG              PIC X VALUE 'N'.
        01  WS-DISPLAY-BAL             PIC ZZZZZZZ9.99.
        01  WS-JSON-OUTPUT             PIC X(100).
        LINKAGE SECTION.
        01  LS-TARGET-ID               PIC X(10).
        PROCEDURE DIVISION USING LS-TARGET-ID.
        0000-MAIN.
            IF LS-TARGET-ID = SPACES
                DISPLAY '{"error": "No ID provided"}'
                STOP RUN
            END-IF.
            OPEN INPUT BALANCE-FILE.
            PERFORM UNTIL WS-EOF-FLAG = 'Y' OR WS-FOUND-FLAG = 'Y'
                READ BALANCE-FILE
                    AT END
                        MOVE 'Y' TO WS-EOF-FLAG
                    NOT AT END
                        IF BR-ACCOUNT-ID = LS-TARGET-ID
                            MOVE BR-BALANCE TO WS-DISPLAY-BAL
                            MOVE SPACES TO WS-JSON-OUTPUT
                            STRING '{"acc":"'
                                   FUNCTION TRIM(LS-TARGET-ID)
                                   '","bal":'
                                   FUNCTION TRIM(WS-DISPLAY-BAL)
                                   '}'
                                   DELIMITED BY SIZE
                                   INTO WS-JSON-OUTPUT
                            DISPLAY FUNCTION TRIM(WS-JSON-OUTPUT)
                            MOVE 'Y' TO WS-FOUND-FLAG
                        END-IF
                END-READ
            END-PERFORM.
            IF WS-FOUND-FLAG = 'N'
                DISPLAY '{"error": "Not found"}'
            END-IF.
            CLOSE BALANCE-FILE.
            STOP RUN.