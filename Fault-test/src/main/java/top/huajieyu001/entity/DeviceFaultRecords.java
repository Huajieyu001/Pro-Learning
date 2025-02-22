package top.huajieyu001.entity;

import com.baomidou.mybatisplus.annotation.TableField;
import com.baomidou.mybatisplus.annotation.TableName;
import com.baomidou.mybatisplus.annotation.IdType;
import java.time.LocalDateTime;
import com.baomidou.mybatisplus.annotation.TableId;
import java.io.Serializable;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.experimental.Accessors;

/**
 * <p>
 * 
 * </p>
 *
 * @author Huajieyu
 * @since 2025-02-19
 */
@Data
@EqualsAndHashCode(callSuper = false)
@Accessors(chain = true)
@TableName("device_fault_records")
public class DeviceFaultRecords implements Serializable {

    private static final long serialVersionUID = 1L;

    @TableId(value = "record_id", type = IdType.AUTO)
    private Long recordId;

    @TableField("fault_id")
    private String faultId;

    @TableField("device_id")
    private Long deviceId;

    @TableField("fault_type")
    private Integer faultType;

    @TableField("fault_level")
    private Integer faultLevel;

    @TableField("occur_time")
    private LocalDateTime occurTime;

    @TableField("op_type")
    private Integer opType;

    @TableField(exist = false)
    private Long millis;

    @Override
    public String toString() {
        return "DeviceFaultRecords{" +
                "recordId=" + recordId +
                ", faultId='" + faultId + '\'' +
                ", deviceId=" + deviceId +
                ", faultType=" + faultType +
                ", faultLevel=" + faultLevel +
                ", occurTime=" + occurTime +
                ", opType=" + opType +
                '}';
    }
}
