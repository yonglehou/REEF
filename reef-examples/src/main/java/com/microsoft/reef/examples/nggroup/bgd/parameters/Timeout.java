package com.microsoft.reef.examples.nggroup.bgd.parameters;

import com.microsoft.tang.annotations.Name;
import com.microsoft.tang.annotations.NamedParameter;


// TODO: Document
@NamedParameter(short_name = "timeout", default_value = "2")
public final class Timeout implements Name<Integer> {
}
