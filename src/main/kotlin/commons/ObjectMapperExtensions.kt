package com.inohoster.commons

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.exc.InvalidDefinitionException

/**
 * This function will use `ObjectMapper.writeValueAsBytes(param)` to check if the type is serializable to JSON format.
 * If the serialization is successful this function wil return true, if this function throws InvalidDefinitionException
 * it will return false. Any other exception will be thrown
 */
fun ObjectMapper.isSerializable(param: Any): Boolean {
    return try {
        this.writeValueAsBytes(param)
        true
    } catch (e: InvalidDefinitionException) {
        false
    } catch (e: Exception) {
        throw e
    }
}