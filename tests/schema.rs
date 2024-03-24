// use diesel::table;
use schema_guard::macros::table_proc;



table! {

    emails (id) {
        id -> Uuid,
        user_id -> Int4,
        email -> Varchar,
        deleted_on -> Nullable<Timestamp>,
    }

}
