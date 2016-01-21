package net.dongliu.requests.struct;

import java.util.List;

/**
 * a list of parameters
 *
 * @author Dong Liu dongliu@live.cn
 */
public class Parameters extends MultiMap<String, String, Parameter> {
    public Parameters() {
    }

    public Parameters(Parameter... pairs) {
        super(pairs);
    }

    public Parameters(List<Parameter> pairs) {
        super(pairs);
    }
}
