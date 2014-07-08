package com.microsoft.reef.examples.nggroup.bgd.parameters;

import com.microsoft.tang.annotations.Name;
import com.microsoft.tang.annotations.NamedParameter;

/**
 * The input folder of the learner.
 */
@NamedParameter(short_name = "input")
public final class InputDir implements Name<String> {
}
