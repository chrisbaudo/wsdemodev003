CREATE FUNCTION dbo.fn_student_rls(@StudentEmail varchar(255))
RETURNS TABLE
WITH SCHEMABINDING
AS
RETURN
(
    SELECT 1 AS AccessResult
    WHERE @StudentEmail = USER_NAME()
);