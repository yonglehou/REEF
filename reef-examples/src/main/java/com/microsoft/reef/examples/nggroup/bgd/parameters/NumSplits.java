package com.microsoft.reef.examples.nggroup.bgd.parameters;

import com.microsoft.tang.annotations.Name;
import com.microsoft.tang.annotations.NamedParameter;

/**
 *
 */
// TODO: Document
@NamedParameter(short_name = "splits", default_value = "5")
public final class NumSplits implements Name<Integer> {
}
