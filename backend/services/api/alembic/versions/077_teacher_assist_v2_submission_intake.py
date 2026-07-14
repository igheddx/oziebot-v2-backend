"""TeacherAssist v2 submission intake + QR matching foundation."""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "077_teacher_assist_v2_submission_intake"
down_revision = "076_teacher_assist_v2_assignments"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "teacher_assist_v2_assignment_print_packets",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("tenant_id", sa.Uuid(), nullable=False),
        sa.Column("teacher_user_id", sa.Uuid(), nullable=False),
        sa.Column("assignment_id", sa.Uuid(), nullable=False),
        sa.Column("platform_school_year_id", sa.Uuid(), nullable=False),
        sa.Column("catalog_district_id", sa.Uuid(), nullable=False),
        sa.Column("catalog_school_id", sa.Uuid(), nullable=True),
        sa.Column("catalog_grade_id", sa.Uuid(), nullable=False),
        sa.Column("catalog_subject_id", sa.Uuid(), nullable=False),
        sa.Column("packet_status", sa.String(length=32), nullable=False),
        sa.Column("pages_per_student", sa.Integer(), nullable=False),
        sa.Column("student_count", sa.Integer(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(
            ["assignment_id"], ["teacher_assist_v2_assignments.id"], ondelete="CASCADE"
        ),
        sa.ForeignKeyConstraint(
            ["catalog_district_id"], ["education_districts.id"], ondelete="CASCADE"
        ),
        sa.ForeignKeyConstraint(["catalog_grade_id"], ["education_grades.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(
            ["catalog_school_id"], ["education_schools.id"], ondelete="SET NULL"
        ),
        sa.ForeignKeyConstraint(
            ["catalog_subject_id"], ["education_subjects.id"], ondelete="CASCADE"
        ),
        sa.ForeignKeyConstraint(
            ["platform_school_year_id"], ["education_school_years.id"], ondelete="CASCADE"
        ),
        sa.ForeignKeyConstraint(["teacher_user_id"], ["users.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["tenant_id"], ["tenants.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_ta_v2_print_packets_assignment",
        "teacher_assist_v2_assignment_print_packets",
        ["assignment_id"],
    )

    op.create_table(
        "teacher_assist_v2_assignment_print_pages",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("packet_id", sa.Uuid(), nullable=False),
        sa.Column("assignment_id", sa.Uuid(), nullable=False),
        sa.Column("student_number", sa.Integer(), nullable=False),
        sa.Column("page_number", sa.Integer(), nullable=False),
        sa.Column("qr_payload_json", sa.JSON(), nullable=False),
        sa.Column("qr_token", sa.String(length=128), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(
            ["assignment_id"], ["teacher_assist_v2_assignments.id"], ondelete="CASCADE"
        ),
        sa.ForeignKeyConstraint(
            ["packet_id"], ["teacher_assist_v2_assignment_print_packets.id"], ondelete="CASCADE"
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_ta_v2_print_pages_qr_token",
        "teacher_assist_v2_assignment_print_pages",
        ["qr_token"],
    )
    op.create_index(
        "ix_ta_v2_print_pages_assignment",
        "teacher_assist_v2_assignment_print_pages",
        ["assignment_id"],
    )

    op.create_table(
        "teacher_assist_v2_submission_batches",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("tenant_id", sa.Uuid(), nullable=False),
        sa.Column("teacher_user_id", sa.Uuid(), nullable=False),
        sa.Column("assignment_id", sa.Uuid(), nullable=False),
        sa.Column("platform_school_year_id", sa.Uuid(), nullable=False),
        sa.Column("catalog_district_id", sa.Uuid(), nullable=False),
        sa.Column("catalog_school_id", sa.Uuid(), nullable=True),
        sa.Column("catalog_grade_id", sa.Uuid(), nullable=False),
        sa.Column("catalog_subject_id", sa.Uuid(), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False, server_default="UPLOADED"),
        sa.Column("uploaded_file_key", sa.String(length=512), nullable=False),
        sa.Column("original_filename", sa.String(length=255), nullable=False),
        sa.Column("mime_type", sa.String(length=255), nullable=False),
        sa.Column("file_size", sa.Integer(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(
            ["assignment_id"], ["teacher_assist_v2_assignments.id"], ondelete="CASCADE"
        ),
        sa.ForeignKeyConstraint(
            ["catalog_district_id"], ["education_districts.id"], ondelete="CASCADE"
        ),
        sa.ForeignKeyConstraint(["catalog_grade_id"], ["education_grades.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(
            ["catalog_school_id"], ["education_schools.id"], ondelete="SET NULL"
        ),
        sa.ForeignKeyConstraint(
            ["catalog_subject_id"], ["education_subjects.id"], ondelete="CASCADE"
        ),
        sa.ForeignKeyConstraint(
            ["platform_school_year_id"], ["education_school_years.id"], ondelete="CASCADE"
        ),
        sa.ForeignKeyConstraint(["teacher_user_id"], ["users.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["tenant_id"], ["tenants.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_ta_v2_submission_batches_assignment",
        "teacher_assist_v2_submission_batches",
        ["assignment_id"],
    )

    op.create_table(
        "teacher_assist_v2_student_submissions",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("tenant_id", sa.Uuid(), nullable=False),
        sa.Column("teacher_user_id", sa.Uuid(), nullable=False),
        sa.Column("assignment_id", sa.Uuid(), nullable=False),
        sa.Column("submission_batch_id", sa.Uuid(), nullable=False),
        sa.Column("packet_id", sa.Uuid(), nullable=True),
        sa.Column("platform_school_year_id", sa.Uuid(), nullable=False),
        sa.Column("catalog_district_id", sa.Uuid(), nullable=False),
        sa.Column("catalog_school_id", sa.Uuid(), nullable=True),
        sa.Column("catalog_grade_id", sa.Uuid(), nullable=False),
        sa.Column("catalog_subject_id", sa.Uuid(), nullable=False),
        sa.Column("student_number", sa.Integer(), nullable=True),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("file_key", sa.String(length=512), nullable=False),
        sa.Column("original_filename", sa.String(length=255), nullable=False),
        sa.Column("mime_type", sa.String(length=255), nullable=False),
        sa.Column("file_size", sa.Integer(), nullable=False),
        sa.Column("page_range", sa.String(length=64), nullable=True),
        sa.Column("qr_identifier", sa.String(length=128), nullable=True),
        sa.Column("match_method", sa.String(length=32), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(
            ["assignment_id"], ["teacher_assist_v2_assignments.id"], ondelete="CASCADE"
        ),
        sa.ForeignKeyConstraint(
            ["catalog_district_id"], ["education_districts.id"], ondelete="CASCADE"
        ),
        sa.ForeignKeyConstraint(["catalog_grade_id"], ["education_grades.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(
            ["catalog_school_id"], ["education_schools.id"], ondelete="SET NULL"
        ),
        sa.ForeignKeyConstraint(
            ["catalog_subject_id"], ["education_subjects.id"], ondelete="CASCADE"
        ),
        sa.ForeignKeyConstraint(
            ["packet_id"], ["teacher_assist_v2_assignment_print_packets.id"], ondelete="SET NULL"
        ),
        sa.ForeignKeyConstraint(
            ["platform_school_year_id"], ["education_school_years.id"], ondelete="CASCADE"
        ),
        sa.ForeignKeyConstraint(
            ["submission_batch_id"], ["teacher_assist_v2_submission_batches.id"], ondelete="CASCADE"
        ),
        sa.ForeignKeyConstraint(["teacher_user_id"], ["users.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["tenant_id"], ["tenants.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_ta_v2_student_submissions_assignment",
        "teacher_assist_v2_student_submissions",
        ["assignment_id"],
    )
    op.create_index(
        "ix_ta_v2_student_submissions_batch",
        "teacher_assist_v2_student_submissions",
        ["submission_batch_id"],
    )


def downgrade() -> None:
    op.drop_index(
        "ix_ta_v2_student_submissions_batch", table_name="teacher_assist_v2_student_submissions"
    )
    op.drop_index(
        "ix_ta_v2_student_submissions_assignment",
        table_name="teacher_assist_v2_student_submissions",
    )
    op.drop_table("teacher_assist_v2_student_submissions")
    op.drop_index(
        "ix_ta_v2_submission_batches_assignment", table_name="teacher_assist_v2_submission_batches"
    )
    op.drop_table("teacher_assist_v2_submission_batches")
    op.drop_index(
        "ix_ta_v2_print_pages_assignment", table_name="teacher_assist_v2_assignment_print_pages"
    )
    op.drop_index(
        "ix_ta_v2_print_pages_qr_token", table_name="teacher_assist_v2_assignment_print_pages"
    )
    op.drop_table("teacher_assist_v2_assignment_print_pages")
    op.drop_index(
        "ix_ta_v2_print_packets_assignment", table_name="teacher_assist_v2_assignment_print_packets"
    )
    op.drop_table("teacher_assist_v2_assignment_print_packets")
