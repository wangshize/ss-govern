package org.ss.govern.core.constants;

import lombok.Data;

/**
 * @author wangsz
 * @create 2020-07-17
 **/
@Data
public class Pair<T1,T2> {

    private T1 object1;
    private T2 object2;
}
