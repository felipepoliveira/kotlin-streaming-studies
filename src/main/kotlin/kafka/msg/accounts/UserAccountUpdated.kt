package com.inohoster.kafka.msg.accounts

class UserAccountUpdated(
    /**
     * Primary Key, ID, auto increment field
     */
    val id: Long,

    /**
     * The user email
     */
    val email: String,
)