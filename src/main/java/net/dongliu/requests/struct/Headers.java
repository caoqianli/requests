package net.dongliu.requests.struct;

import java.util.List;

/**
 * a list of headers
 *
 * @author Dong Liu dongliu@live.cn
 */
public class Headers extends MultiMap<String, String, Header> {
    public Headers() {
    }

    public Headers(Header... pairs) {
        super(pairs);
    }

    public Headers(List<Header> pairs) {
        super(pairs);
    }
}
