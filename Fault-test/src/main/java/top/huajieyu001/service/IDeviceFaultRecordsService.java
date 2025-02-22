package top.huajieyu001.service;

import top.huajieyu001.dto.FaultResult;
import top.huajieyu001.entity.DeviceFaultRecords;
import com.baomidou.mybatisplus.extension.service.IService;

import java.time.LocalDateTime;
import java.util.List;

/**
 * <p>
 *  服务类
 * </p>
 *
 * @author Huajieyu
 * @since 2025-02-19
 */
public interface IDeviceFaultRecordsService extends IService<DeviceFaultRecords> {

    public List<FaultResult> execute(String startTimeStr, String endTimeStr);

    public List<FaultResult> execute2(LocalDateTime startTime, LocalDateTime endTime);
}
